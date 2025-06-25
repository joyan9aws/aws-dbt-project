-- Clean regex columns (remove tabs from query_type)
SELECT
    *,
    REGEXP_REPLACE(query_type, '\t', '') AS query_type_clean
FROM {{ ref('cleaned') }}
