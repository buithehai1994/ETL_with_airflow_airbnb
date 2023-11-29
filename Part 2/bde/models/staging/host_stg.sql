{{ config(
    strategy = 'timestamp',
    unique_key = ['dbt_scd_id'],
    updated_at = ['scraped_date'],
    check_cols = 'all'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'host_snapshot') }}
)

SELECT
    id,
    host_name,
    HOST_SINCE,
    SCRAPED_DATE,
    host_is_superhost,
    CASE
        WHEN HOST_NEIGHBOURHOOD = 'NaN' OR HOST_NEIGHBOURHOOD IS NULL THEN 'Unknown'
        ELSE HOST_NEIGHBOURHOOD
    END AS HOST_NEIGHBOURHOOD
FROM source
WHERE scraped_date IS NOT NULL 
    AND host_since IS NOT NULL
    AND SCRAPED_DATE >='01/05/2020' AND SCRAPED_DATE <'01/05/2021'