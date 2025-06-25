-- Category dimension
SELECT
    category,
    ROW_NUMBER() OVER (ORDER BY category) AS category_id
FROM (
    SELECT DISTINCT category FROM {{ ref('pii_masked') }} WHERE category IS NOT NULL
)
