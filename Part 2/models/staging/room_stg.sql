{{ config(
    strategy = 'timestamp',
    unique_key = ['dbt_scd_id'],
    updated_at = ['scraped_date'],
    check_cols = 'all'
) }}

WITH source AS (
    SELECT * FROM {{ ref('room_snapshot') }}
)

SELECT
    id,
    SCRAPED_DATE,
    COALESCE(room_type, Null) as room_type,
    COALESCE(accommodates, Null) as accommodates,
    PRICE,
    HAS_AVAILABILITY
FROM source
WHERE scraped_date IS NOT NULL 
    AND room_type IS NOT NULL
    AND accommodates IS NOT NULL
    AND SCRAPED_DATE >= '01/05/2020' AND SCRAPED_DATE < '01/05/2021'
