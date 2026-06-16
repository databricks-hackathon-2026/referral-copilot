from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Create Table Example") \
    .getOrCreate()

# Execute SQL to create an empty table
spark.sql("""
    CREATE OR REPLACE TABLE workspace.default.sos_facility_index AS
    SELECT
    unique_id,
    name,
    organization_type,
    phone_numbers,
    officialPhone,
    email,
    websites,
    officialWebsite,
    address_line1,
    address_line2,
    address_city,
    address_stateOrRegion,
    address_zipOrPostcode,
    address_country,
    latitude as facility_latitude,
    longitude as facility_longitude,
    district,
    state,
    location_confidence,
    coord_nearest_pin_distance_km,
    postcode as verified_postcode
    ai_query(
        'databricks-gpt-oss-120b',
        CONCAT(
        'You are a medical specialties expert. Standardize the following specialties using the conventions (https://careersinmedicine.aamc.org/explore-options/specialty-profiles) as the gold standard. ',
        'Return ONLY a valid JSON array in this exact format: ["standardized_name", ...]. ',
        'Rules: ',
        '1. Combine similar/duplicate services into single standardized terms ',
        '2. Use camelCase for specialty names ',
        '3. Ensure to use AAMC-based valid specialties ',
        '4. For specialties with brand names, standardize to generic name ',
        'Input JSON array: ', specialties, '. ',
        'Output only the JSON array, no explanation.'
        )
    ) AS specialties,
    ai_query(
        'databricks-gpt-oss-120b',
        CONCAT(
        'You are a medical terminology expert. Standardize the following medical procedures and services using Mayo Clinic naming conventions (https://www.mayoclinic.org/tests-procedures/index) as the gold standard. ',
        'Return ONLY a valid JSON array in this exact format: ["standardized_name|category", ...]. ',
        'Rules: ',
        '1. Use Mayo Clinic official procedure names when available (e.g., "Hip replacement" → "Hip Replacement") ',
        '2. Remove all facility names, metadata, and non-medical content ',
        '3. Combine similar/duplicate services into single standardized terms ',
        '4. Use Title Case for procedure names ',
        '5. Keep precise medical terminology ',
        '6. Categorize each as: Surgery, Diagnostic, Treatment, or Support Service ',
        '7. For robotic-assisted procedures, standardize as the base procedure (e.g., "Robotic knee replacement" → "Knee Replacement|Surgery") ',
        'Input JSON array: ', procedure, '. ',
        'Output only the JSON array, no explanation.'
        )
    ) AS standardized_services,
    ai_query(
        'databricks-gpt-oss-120b',
        CONCAT(
        'You are a medical equipment classification expert. Standardize the following medical equipment using Wikipedia medical equipment naming conventions (https://en.wikipedia.org/wiki/Category:Medical_equipment) as the gold standard. ',
        'Return ONLY a valid JSON array in this exact format: ["standardized_equipment_name|category", ...]. ',
        'Rules: ',
        '1. Use Wikipedia official medical equipment names when available (e.g., "CT scanner" → "CT Scanner") ',
        '2. Remove quantities, facility names, metadata, and non-equipment content ',
        '3. Combine similar/duplicate equipment into single standardized terms ',
        '4. Use Title Case for equipment names ',
        '5. Keep precise medical terminology ',
        '6. Categorize each as: Imaging Equipment, Life Support Equipment, Surgical Equipment, Diagnostic Equipment, Laboratory Equipment, or Medical Furniture ',
        '7. For equipment with brand names, standardize to generic name ',
        '8. Exclude non-medical infrastructure (parking, buildings, staff counts) ',
        'Input JSON array: ', equipment, '. ',
        'Output only the JSON array, no explanation.'
        )
    ) AS standardized_equipment,
    ai_query(
        'databricks-gpt-oss-120b',
        CONCAT(
        'You are a healthcare capability analyst preparing data for AI agent consumption. ',
        'Parse and structure the following medical facility capability information. ',
        'Return a JSON object with this exact structure: ',
        '{"capabilities": [{"capability_name": "<name>", "category": "<Clinical|Operational|Technical|Infrastructure|Educational|Research>", "description": "<description>", "complexity_level": "<Basic|Intermediate|Advanced|Specialized>", "ai_agent_tags": ["<tags>"]}], "primary_focus": "<focus>", "specialization_level": "<General|Specialized|Tertiary|Quaternary>", "summary": "<150 char summary>"} ',
        'Rules: 1. Standardize capability names 2. Use specified categories 3. Assign complexity levels 4. Add AI agent tags 5. Identify primary focus 6. Assess specialization level 7. Create concise summary 8. Remove redundancy 9. Include metrics. ',
        'Input capability data: ', capability
        )
    ) AS parsed_capability
