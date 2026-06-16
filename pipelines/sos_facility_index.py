# Databricks notebook source
# gold.referral_copilot_facilities
#
# One row per facility. Consolidates:
#   - facilities (base fields: identity, contact, address, coordinates)
#   - facilities_specialties_cleansed_ai (AI-deduped specialties)
#   - facilities_standardized_services_ai (standardized services)
#   - facilities_wikipedia_standardized_equipment (standardized equipment)
#   - facilities_parsed_capabilities (parsed capability)
#   - referral_copilot_facility_location (location enrichment: district/state/confidence)
#
# All joins are LEFT joins on unique_id so facilities with no match in a
# teammate table still appear in Gold (with nulls for that table's columns)
# rather than being silently dropped.
#
# referral_copilot_facility_coord_nearest_pin and
# referral_copilot_pincode_district_lookup are intentionally excluded --
# their relevant columns are already present in referral_copilot_facility_location.

from pyspark.sql import functions as F

FACILITIES        = "databricks_virtue_foundation_dataset_dais_2026.virtue_foundation_dataset.facilities"
# facilities_specialties_cleansed_ai is a full copy of facilities (51 cols) --
# pulling specialties directly from FACILITIES base table instead, no join needed.
SERVICES          = "workspace.default.facilities_standardized_services_ai"
EQUIPMENT         = "workspace.default.facilities_wikipedia_standardized_equipment"
CAPABILITIES      = "workspace.default.facilities_parsed_capabilities"
LOCATION          = "workspace.default.referral_copilot_facility_location"
TARGET_TABLE      = "workspace.default.sos_facility_index"

# --- Base: curated columns from facilities ---
facilities = spark.table(FACILITIES).select(
    "unique_id",
    "name",
    "organization_type",
    "phone_numbers",
    "officialPhone",
    "email",
    "websites",
    "officialWebsite",
    "numberDoctors",
    "capacity",
    "description",
    "address_line1",
    "address_line2",
    "address_city",
    "address_stateOrRegion",
    "address_zipOrPostcode",
    "address_country",
    "specialties",  # pulled directly -- facilities_specialties_cleansed_ai is a full copy
    F.col("latitude").alias("facility_latitude"),
    F.col("longitude").alias("facility_longitude"),
)

# --- Teammate tables ---
services = spark.table(SERVICES).select(
    F.col("unique_id").alias("svc_uid"),
    "standardized_services",
)

equipment = spark.table(EQUIPMENT).select(
    F.col("unique_id").alias("eq_uid"),
    "standardized_equipment",
)

capabilities = spark.table(CAPABILITIES).select(
    F.col("unique_id").alias("cap_uid"),
    "parsed_capability",
)

# --- Your location enrichment table ---
location = spark.table(LOCATION).select(
    F.col("facility_id").alias("loc_uid"),
    "district",
    "state",
    "location_confidence",
    "coord_nearest_pin_distance_km",
    F.col("postcode").alias("verified_postcode"),
)

# --- Join everything on unique_id ---
gold = (
    facilities
    .join(services,     facilities.unique_id == services.svc_uid,     "left")
    .join(equipment,    facilities.unique_id == equipment.eq_uid,     "left")
    .join(capabilities, facilities.unique_id == capabilities.cap_uid, "left")
    .join(location,     facilities.unique_id == location.loc_uid,     "left")
    .select(
        # Identity
        facilities.unique_id,
        "name",
        "organization_type",
        # Contact
        "phone_numbers",
        "officialPhone",
        "email",
        "websites",
        "officialWebsite",
        # Address (raw)
        "address_line1",
        "address_line2",
        "address_city",
        "address_stateOrRegion",
        "address_zipOrPostcode",
        "address_country",
        # Coordinates (operational -- primary for distance ranking)
        "facility_latitude",
        "facility_longitude",
        # Location enrichment (display + confidence)
        "district",
        "state",
        "location_confidence",
        "coord_nearest_pin_distance_km",
        "verified_postcode",
        # Capacity
        "numberDoctors",
        "capacity",
        # Description
        "description",
        # Care-need matching (from teammate tables)
        "specialties",
        "standardized_services",
        "standardized_equipment",
        "parsed_capability",
    )
)

# --- Sanity checks ---
total = gold.count()
print(f"Total rows in Gold table: {total} (expect ~10,000)")

null_location = gold.filter(F.col("district").isNull()).count()
print(f"Facilities with null district (unresolved location): {null_location}")

null_services = gold.filter(F.col("standardized_services").isNull()).count()
print(f"Facilities with null standardized_services: {null_services}")

# --- Write ---
(
    gold.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Written to {TARGET_TABLE}")
