-- Fact table
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
FROM {{ ref('pii_masked') }} pm
LEFT JOIN {{ ref('dim_category') }} dc ON pm.category = dc.category
LEFT JOIN {{ ref('dim_sector') }} dsec ON pm.sector = dsec.sector
LEFT JOIN {{ ref('dim_state') }} ds ON pm.state_name = ds.state_name
