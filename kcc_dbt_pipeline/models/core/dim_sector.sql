-- Sector dimension
SELECT
    sector,
    ROW_NUMBER() OVER (ORDER BY sector) AS sector_id
FROM (
    SELECT DISTINCT sector FROM {{ ref('pii_masked') }} WHERE sector IS NOT NULL
)
