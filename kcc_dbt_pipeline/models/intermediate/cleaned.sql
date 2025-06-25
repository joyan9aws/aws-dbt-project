-- Remove invalid values from categorical columns
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
