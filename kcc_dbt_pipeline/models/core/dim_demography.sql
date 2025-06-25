-- Demography dimension
SELECT
    state_name,
    ARRAY_AGG(DISTINCT district_name) AS district_names,
    ARRAY_AGG(DISTINCT block_name) AS block_names
FROM {{ ref('pii_masked') }}
GROUP BY state_name
