-- Mask PII in kcc_ans (mask phone numbers and emails)
SELECT
    *,
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            kcc_ans,
            '(\\+91[\\-\\s]?\\d{10}|\\b\\d{10}\\b)', '[PHONE]'
        ),
        '[a-zA-Z0-9.\\-_]+@[a-zA-Z0-9\\-_]+\\.[a-zA-Z.]+' , '[EMAIL]'
    ) AS kcc_ans_masked
FROM {{ ref('regex_cleaned') }}
