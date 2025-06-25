WITH
-- 1. Remove invalid values from categorical columns
cleaned AS (
    SELECT
        *,
        NULLIF(TRIM(state_name), 'NA') AS state_name,
        NULLIF(TRIM(district_name), 'NA') AS district_name,
        NULLIF(TRIM(block_name), 'NA') AS block_name,
        NULLIF(TRIM(category), '0') AS category,
        NULLIF(TRIM(season), 'NA') AS season
    FROM {{ ref('stg_kcc_data') }}
    WHERE state_name IS NOT NULL
      AND district_name IS NOT NULL
      AND block_name IS NOT NULL
      AND category IS NOT NULL
      AND season IS NOT NULL
),

-- 2. Clean regex columns (example: remove tabs from query_type)
regex_cleaned AS (
    SELECT
        *,
        REGEXP_REPLACE(query_type, '\t', '') AS query_type_clean
    FROM cleaned
),

-- 3. Mask PII in kcc_ans (example: mask phone numbers and emails)
pii_masked AS (
    SELECT
        *,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                kcc_ans,
                '(\\+91[\\-\\s]?\\d{10}|\\b\\d{10}\\b)', '[PHONE]'
            ),
            '[a-zA-Z0-9.\\-_]+@[a-zA-Z0-9\\-_]+\\.[a-zA-Z.]+', '[EMAIL]'
        ) AS kcc_ans_masked
    FROM regex_cleaned
),

-- 4. Dimension tables
dim_category AS (
    SELECT
        category,
        ROW_NUMBER() OVER (ORDER BY category) AS category_id
    FROM (
        SELECT DISTINCT category FROM pii_masked WHERE category IS NOT NULL
    )
),
dim_sector AS (
    SELECT
        sector,
        ROW_NUMBER() OVER (ORDER BY sector) AS sector_id
    FROM (
        SELECT DISTINCT sector FROM pii_masked WHERE sector IS NOT NULL
    )
),
dim_state AS (
    SELECT
        state_name,
        ROW_NUMBER() OVER (ORDER BY state_name) AS state_id
    FROM (
        SELECT DISTINCT state_name FROM pii_masked WHERE state_name IS NOT NULL
    )
),
dim_demography AS (
    SELECT
        state_name,
        ARRAY_AGG(DISTINCT district_name) AS district_names,
        ARRAY_AGG(DISTINCT block_name) AS block_names
    FROM pii_masked
    GROUP BY state_name
),

-- 5. Fact table
fact_queries AS (
    SELECT
        pm._dlt_id AS query_id,
        pm.created_on,
        ds.state_id,
        dc.category_id,
        dsec.sector_id,
        pm.crop,
        pm.query_type_clean AS query_type,
        pm.query_text,
        pm.kcc_ans_masked AS kcc_ans
    FROM pii_masked pm
    LEFT JOIN dim_category dc ON pm.category = dc.category
    LEFT JOIN dim_sector dsec ON pm.sector = dsec.sector
    LEFT JOIN dim_state ds ON pm.state_name = ds.state_name
)

SELECT * FROM fact_queries
