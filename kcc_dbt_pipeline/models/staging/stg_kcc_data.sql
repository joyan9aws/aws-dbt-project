SELECT
    -- Categorical cleaning
    CASE 
        WHEN state_name IN ('NA', '0') OR state_name IS NULL THEN 'Not Available'
        ELSE state_name
    END AS state_name,

    CASE 
        WHEN district_name IN ('NA', '9999') OR district_name IS NULL THEN 'Not Available'
        ELSE district_name
    END AS district_name,

    CASE 
        WHEN block_name IN ('NA', '0   ') OR block_name IS NULL THEN 'Not Available'
        ELSE block_name
    END AS block_name,

    -- Regex cleaning (DuckDB version)
    CASE 
        WHEN season IN ('NA') OR season IS NULL THEN 'Not Available'
        ELSE season
    END AS season,

    CASE 
        WHEN REGEXP_MATCHES(sector, '^[0-9]+$') OR sector IN ('NA', '0') OR sector IS NULL THEN 'Not Available'
        ELSE sector
    END AS sector,

    CASE 
        WHEN REGEXP_MATCHES(crop, '^[0-9]+$') OR crop IN ('NA', '0') OR crop IS NULL THEN 'Not Available'
        ELSE crop
    END AS crop,

    CASE 
        WHEN REGEXP_MATCHES(query_type, '^[0-9]+$') OR query_type IN ('NA', '0') OR query_type IS NULL THEN 'Not Available'
        ELSE query_type
    END AS query_type,

    CASE 
        WHEN REGEXP_MATCHES(category, '^[0-9]+$') OR category IN ('0') OR category IS NULL THEN 'Not Available'
        ELSE category
    END AS category,

    -- PII masking
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                query_text,
                '(\\+91[\\-\\s]?\\d{10})|(\\b\\d{10}\\b)', 
                '[PHONE]'
            ),
            '[a-zA-Z0-9.\\-_]+@[a-zA-Z0-9\\-_]+\\.[a-zA-Z.]+', 
            '[EMAIL]'
        ),
        '\\b\\d{9,18}\\b', 
        '[ACCOUNT]'
    ) AS query_text,

    kcc_ans,
    CAST(created_on AS TIMESTAMP) AS created_on,
    CAST(year AS INT) AS year,
    CAST(month AS INT) AS month,
    _dlt_load_id,
    _dlt_id

FROM {{ source('kcc_raw_data', 'kcc_data') }}
