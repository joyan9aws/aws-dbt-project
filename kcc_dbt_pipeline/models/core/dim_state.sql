-- State dimension
SELECT
    state_name,
    ROW_NUMBER() OVER (ORDER BY state_name) AS state_id
FROM (
    SELECT DISTINCT state_name FROM {{ ref('pii_masked') }} WHERE state_name IS NOT NULL
)
