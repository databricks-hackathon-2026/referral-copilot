# Referral Copilot - location sanitization pipeline
# Reads (shared, read-only): databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset
# Writes (own workspace):    workspace.default.referral_copilot_*
#
# Import this file into Databricks (Workspace -> Import) -- the
# COMMAND ---------- markers will become separate cells. Run top to bottom.

# COMMAND ----------
# DBTITLE 1,01 - pincode_district_lookup
# silver.pincode_district_lookup
#
# Source: databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.india_post_pincode_directory
# Row grain in source: one row per post office (a single PIN can span multiple
# post offices, and those post offices can carry different district values).
# This transform collapses to one row per pincode with:
#   - modal (most frequent) district + statename for that PIN
#   - district_match_count: number of DISTINCT districts seen for this PIN
#       (1 = unambiguous, >1 = ambiguous -- flag downstream)
#   - mean lat/lon across post offices that have coordinates
#   - has_coordinates: whether at least one post office for this PIN has
#       non-NA lat/lon
#   - office_count: total post offices collapsed into this row (informational)
#
# Known source quirks handled here:
#   - latitude/longitude arrive as the literal string "NA" for ~12,600 of
#     165,627 rows, not SQL NULL -- must be cast explicitly.
#   - district casing is inconsistent even within the source table itself
#     (e.g. "KUMURAM BHEEM ASIFABAD" vs "Jagitial" vs "RAJANNA SIRCILLA").
#     We uppercase + trim district/statename before grouping so that casing
#     differences don't create spurious "different district" rows for the
#     same PIN.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.india_post_pincode_directory"
TARGET_TABLE = "workspace.default.referral_copilot_pincode_district_lookup"

raw = spark.table(SOURCE_TABLE)

# --- Normalize fields before aggregation ---
# latitude/longitude are stored as strings and are NOT uniformly decimal degrees:
#   - "NA" for missing values (~12,600 of 165,627 rows)
#   - some rows contain DMS-style fragments (e.g. "17\u00b057'17.7"), which are not
#     valid decimal-degree doubles and may themselves be truncated/malformed.
# try_cast() returns NULL for anything that doesn't parse as a double, so both
# "NA" and DMS-style/malformed strings end up as NULL here -- treated the same
# as "no coordinates available for this row" downstream.
normalized = raw.select(
    F.col("pincode").cast("string").alias("pincode"),
    F.trim(F.upper(F.col("district"))).alias("district_norm"),
    F.trim(F.upper(F.col("statename"))).alias("statename_norm"),
    F.expr("try_cast(latitude AS DOUBLE)").alias("latitude"),
    F.expr("try_cast(longitude AS DOUBLE)").alias("longitude"),
)

# --- Per (pincode, district, state) counts, to find the modal district ---
district_counts = (
    normalized
    .groupBy("pincode", "district_norm", "statename_norm")
    .agg(F.count(F.lit(1)).alias("office_count_for_district"))
)

# --- Rank districts within each pincode by frequency (most common first) ---
# Ties broken deterministically by district_norm alphabetical order so the
# result is stable across runs.
ranked = district_counts.withColumn(
    "rank",
    F.row_number().over(
        Window
        .partitionBy("pincode")
        .orderBy(F.col("office_count_for_district").desc(), F.col("district_norm").asc())
    ),
)

modal_district = (
    ranked.filter(F.col("rank") == 1)
    .select(
        "pincode",
        F.col("district_norm").alias("district"),
        F.col("statename_norm").alias("statename"),
    )
)

# --- Ambiguity signal: how many DISTINCT districts does this pincode span? ---
district_ambiguity = (
    normalized
    .groupBy("pincode")
    .agg(F.countDistinct("district_norm").alias("district_match_count"))
)

# --- Coordinate aggregation: mean of non-null lat/lon, plus has_coordinates flag ---
coords = (
    normalized
    .groupBy("pincode")
    .agg(
        F.avg("latitude").alias("latitude"),
        F.avg("longitude").alias("longitude"),
        F.max(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
         .alias("has_coordinates"),
        F.count(F.lit(1)).alias("office_count"),
    )
)

result = (
    modal_district
    .join(district_ambiguity, "pincode", "left")
    .join(coords, "pincode", "left")
    .select(
        "pincode",
        "district",
        "statename",
        "district_match_count",
        "latitude",
        "longitude",
        "has_coordinates",
        "office_count",
    )
)

# --- Sanity checks before write ---
total_pincodes = result.count()
ambiguous_pincodes = result.filter(F.col("district_match_count") > 1).count()
no_coords = result.filter(~F.col("has_coordinates")).count()

print(f"Distinct PIN codes: {total_pincodes}")
print(f"PIN codes spanning >1 district (ambiguous): {ambiguous_pincodes}")
print(f"PIN codes with no coordinates available: {no_coords}")

(
    result.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------
# DBTITLE 1,02 - facility_coord_nearest_pin
# Coordinate-based reverse lookup: for each facility, find the nearest
# PIN-directory point (from silver.pincode_district_lookup, restricted to PINs
# that have coordinates) and return that PIN's district/state + distance.
#
# This is intentionally independent of address_zipOrPostcode -- it derives a
# location purely from the facility's own latitude/longitude, which is the
# "primary, operational" signal per the design decision. It gives a second,
# independent district/state estimate to cross-check against the
# postcode-derived one in 03_facility_location.py.
#
# Performance note: a naive cross join is ~10,000 facilities x ~19,586 PINs
# (~196M pairs), too heavy for a 2X-Small warehouse. Instead we bucket both
# sides into ~0.1-degree (~11km) grid cells and join on a 3x3 cell
# neighborhood, then compute exact haversine only on those candidates.
# A facility whose true nearest PIN falls just outside the 3x3 window (~33km
# radius) could in principle be missed -- acceptable for this use case, since
# anything that far from any reference PIN should land in 'unresolved'
# territory regardless (see 03_facility_location.py).

from pyspark.sql import functions as F
from pyspark.sql.window import Window

FACILITIES_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
PINCODE_LOOKUP_TABLE = "workspace.default.referral_copilot_pincode_district_lookup"
TARGET_TABLE = "workspace.default.referral_copilot_facility_coord_nearest_pin"

GRID_SIZE = 0.1  # degrees, ~11km

# --- Facilities: keep only those with usable coordinates ---
facilities = (
    spark.table(FACILITIES_TABLE)
    .select(
        F.col("unique_id").alias("facility_id"),
        F.expr("try_cast(latitude AS DOUBLE)").alias("latitude"),
        F.expr("try_cast(longitude AS DOUBLE)").alias("longitude"),
    )
    .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    .withColumn("grid_lat", F.floor(F.col("latitude") / GRID_SIZE))
    .withColumn("grid_lon", F.floor(F.col("longitude") / GRID_SIZE))
)

# --- PIN reference points: only PINs with aggregated coordinates ---
pin_points = (
    spark.table(PINCODE_LOOKUP_TABLE)
    .filter(F.col("has_coordinates"))
    .select(
        "pincode",
        "district",
        "statename",
        F.col("latitude").alias("pin_latitude"),
        F.col("longitude").alias("pin_longitude"),
    )
    .withColumn("grid_lat", F.floor(F.col("pin_latitude") / GRID_SIZE))
    .withColumn("grid_lon", F.floor(F.col("pin_longitude") / GRID_SIZE))
)

# --- Expand each facility to its 3x3 neighborhood of grid cells ---
offsets = spark.createDataFrame(
    [(dx, dy) for dx in (-1, 0, 1) for dy in (-1, 0, 1)],
    ["dx", "dy"],
)

facility_cells = (
    facilities
    .crossJoin(offsets)
    .withColumn("cell_lat", F.col("grid_lat") + F.col("dx"))
    .withColumn("cell_lon", F.col("grid_lon") + F.col("dy"))
    .select("facility_id", "latitude", "longitude", "cell_lat", "cell_lon")
)

# --- Join candidates on matching grid cell ---
candidates = facility_cells.join(
    pin_points,
    (facility_cells.cell_lat == pin_points.grid_lat)
    & (facility_cells.cell_lon == pin_points.grid_lon),
    "inner",
)

# --- Haversine distance (km) ---
R_KM = 6371.0

def haversine_km(lat1, lon1, lat2, lon2):
    lat1_r, lon1_r, lat2_r, lon2_r = (
        F.radians(lat1), F.radians(lon1), F.radians(lat2), F.radians(lon2)
    )
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = (
        F.sin(dlat / 2) ** 2
        + F.cos(lat1_r) * F.cos(lat2_r) * F.sin(dlon / 2) ** 2
    )
    return 2 * R_KM * F.asin(F.sqrt(a))

candidates_with_dist = candidates.withColumn(
    "distance_km",
    haversine_km(
        F.col("latitude"), F.col("longitude"),
        F.col("pin_latitude"), F.col("pin_longitude"),
    ),
)

# --- Take the nearest PIN per facility ---
window = Window.partitionBy("facility_id").orderBy(F.col("distance_km").asc())

nearest = (
    candidates_with_dist
    .withColumn("rank", F.row_number().over(window))
    .filter(F.col("rank") == 1)
    .select(
        "facility_id",
        F.col("district").alias("coord_nearest_pin_district"),
        F.col("statename").alias("coord_nearest_pin_state"),
        F.col("pincode").alias("coord_nearest_pincode"),
        F.round("distance_km", 2).alias("coord_nearest_pin_distance_km"),
    )
)

# --- Facilities with no match in their 3x3 neighborhood (sparse PIN coverage) ---
matched_ids = nearest.select("facility_id")
all_with_coords = facilities.select("facility_id")
unmatched = all_with_coords.join(matched_ids, "facility_id", "left_anti")

unmatched_count = unmatched.count()
total_with_coords = all_with_coords.count()
matched_count = nearest.count()

print(f"Facilities with coordinates: {total_with_coords}")
print(f"Matched to a nearest PIN within 3x3 grid neighborhood: {matched_count}")
print(f"Unmatched (no reference PIN within ~33km grid window): {unmatched_count}")

if unmatched_count > 0:
    far_unresolved = unmatched.withColumn("coord_nearest_pin_district", F.lit(None).cast("string")) \
        .withColumn("coord_nearest_pin_state", F.lit(None).cast("string")) \
        .withColumn("coord_nearest_pincode", F.lit(None).cast("string")) \
        .withColumn("coord_nearest_pin_distance_km", F.lit(None).cast("double")) \
        .select("facility_id", "coord_nearest_pin_district", "coord_nearest_pin_state",
                "coord_nearest_pincode", "coord_nearest_pin_distance_km")
    nearest = nearest.unionByName(far_unresolved)

(
    nearest.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------
# DBTITLE 1,03 - facility_location
# silver.facility_location
#
# One row per facility. Combines two independent location signals:
#   1. pin_district / pin_state       -- via facility's stated postcode
#                                          (silver.pincode_district_lookup)
#   2. coord_nearest_pin_district/...  -- via facility's own lat/long, reverse
#                                          looked-up against PIN reference points
#                                          (silver.facility_coord_nearest_pin)
#
# Design principle (per project decision): facility latitude/longitude is the
# PRIMARY, operational location -- used for all distance/ranking in the app.
# Everything in this table is supplementary metadata for DISPLAY and
# location_confidence labeling. Nothing here overrides latitude/longitude.
#
# location_confidence values:
#   - "confirmed"        : postcode resolves to a PIN (district_match_count == 1)
#                           AND pin_district == coord_nearest_pin_district.
#                           Both signals agree -- district/state label trustworthy.
#   - "coordinate_based"  : postcode missing / no PIN match / ambiguous PIN /
#                           pin_district != coord_nearest_pin_district, but a
#                           coordinate-based match exists and is plausible
#                           (coord_nearest_pin_distance_km <= MAX_PLAUSIBLE_KM).
#                           Falls back to the coordinate-derived district/state;
#                           stated postcode did not corroborate it.
#   - "ambiguous_pin"     : postcode resolves but to a PIN spanning >1 district
#                           (district_match_count > 1), and the coordinate-based
#                           signal could not disambiguate (missing or itself
#                           implausible). pin_district shown with caveat.
#   - "unresolved"        : no postcode, no PIN match, no coordinates, or the
#                           nearest reference PIN is implausibly far
#                           (> MAX_PLAUSIBLE_KM). district/state left null --
#                           app should display "location unknown" rather than guess.
#
# district/state resolution order:
#   confirmed / coordinate_based -> coord_nearest_pin_district/state
#   ambiguous_pin                -> pin_district/state (with caveat)
#   unresolved                   -> null

from pyspark.sql import functions as F

FACILITIES_TABLE = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
PINCODE_LOOKUP_TABLE = "workspace.default.referral_copilot_pincode_district_lookup"
COORD_NEAREST_TABLE = "workspace.default.referral_copilot_facility_coord_nearest_pin"
TARGET_TABLE = "workspace.default.referral_copilot_facility_location"

# Beyond this, treat the "nearest" reference PIN as too far to be meaningful
# (sparse PIN coverage / facility coordinates likely off-continent or bad).
MAX_PLAUSIBLE_KM = 50.0

# --- Base: facilities with passthrough coordinates + postcode ---
facilities = spark.table(FACILITIES_TABLE).select(
    F.col("unique_id").alias("facility_id"),
    F.expr("try_cast(latitude AS DOUBLE)").alias("latitude"),
    F.expr("try_cast(longitude AS DOUBLE)").alias("longitude"),
    F.col("address_zipOrPostcode").alias("postcode"),
)

# --- Postcode-based signal ---
pincode_lookup = spark.table(PINCODE_LOOKUP_TABLE).select(
    F.col("pincode"),
    F.col("district").alias("pin_district"),
    F.col("statename").alias("pin_state"),
    F.col("district_match_count").alias("pin_district_match_count"),
)

with_pin = facilities.join(
    pincode_lookup,
    facilities.postcode == pincode_lookup.pincode,
    "left",
)

# --- Coordinate-based signal ---
coord_nearest = spark.table(COORD_NEAREST_TABLE)

with_coord = with_pin.join(coord_nearest, "facility_id", "left")

# --- Derive confidence + resolved district/state ---
has_postcode = F.col("postcode").isNotNull()
has_pin_match = F.col("pin_district").isNotNull()
pin_unambiguous = F.col("pin_district_match_count") == 1
has_coord_match = (
    F.col("coord_nearest_pin_district").isNotNull()
    & (F.col("coord_nearest_pin_distance_km") <= MAX_PLAUSIBLE_KM)
)
districts_agree = (
    F.upper(F.trim(F.col("pin_district")))
    == F.upper(F.trim(F.col("coord_nearest_pin_district")))
)

result = with_coord.withColumn(
    "location_confidence",
    F.when(
        has_postcode & has_pin_match & pin_unambiguous & has_coord_match & districts_agree,
        F.lit("confirmed"),
    )
    .when(
        has_coord_match
        & (
            ~has_postcode
            | ~has_pin_match
            | ~pin_unambiguous
            | ~districts_agree
        ),
        F.lit("coordinate_based"),
    )
    .when(
        has_postcode & has_pin_match & ~pin_unambiguous,
        F.lit("ambiguous_pin"),
    )
    .otherwise(F.lit("unresolved")),
)

result = result.withColumn(
    "district",
    F.when(
        F.col("location_confidence").isin("confirmed", "coordinate_based"),
        F.col("coord_nearest_pin_district"),
    )
    .when(F.col("location_confidence") == "ambiguous_pin", F.col("pin_district"))
    .otherwise(F.lit(None).cast("string")),
).withColumn(
    "state",
    F.when(
        F.col("location_confidence").isin("confirmed", "coordinate_based"),
        F.col("coord_nearest_pin_state"),
    )
    .when(F.col("location_confidence") == "ambiguous_pin", F.col("pin_state"))
    .otherwise(F.lit(None).cast("string")),
)

final = result.select(
    "facility_id",
    "latitude",
    "longitude",
    "postcode",
    "pin_district",
    "pin_state",
    "pin_district_match_count",
    "coord_nearest_pin_district",
    "coord_nearest_pin_state",
    "coord_nearest_pin_distance_km",
    "district",
    "state",
    "location_confidence",
)

# --- Sanity checks ---
total = final.count()
by_confidence = (
    final.groupBy("location_confidence")
    .count()
    .orderBy(F.col("count").desc())
    .collect()
)

print(f"Total facilities: {total}")
for row in by_confidence:
    pct = 100.0 * row["count"] / total if total else 0
    print(f"  {row['location_confidence']}: {row['count']} ({pct:.1f}%)")

(
    final.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)
