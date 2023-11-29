{{ config(
    strategy = 'timestamp',
    unique_key = ['dbt_scd_id'],
    updated_at = ['scraped_date'],
    check_cols = 'all'
) }}

WITH source AS (
    SELECT * FROM {{ ref('property_snapshot') }}
)

SELECT
    id,
    PROPERTY_TYPE,
    SCRAPED_DATE
FROM source
WHERE scraped_date IS NOT NULL 
AND SCRAPED_DATE >= '01/05/2020' AND SCRAPED_DATE < '01/05/2021'
