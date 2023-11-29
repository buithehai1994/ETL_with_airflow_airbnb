-- Snapshot for room_snapshot
{% snapshot room_snapshot %}
{{
    config(
        target_schema='raw',
        materialized='snapshot',
        unique_key='id',
        strategy='timestamp',
        updated_at='SCRAPED_DATE'
    )
}}

-- Select and populate the snapshot with data from the 'listings' source table
SELECT distinct
    MD5(ROOM_TYPE) as id,
    CASE
      WHEN SCRAPED_DATE ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD')
      WHEN SCRAPED_DATE ~ '^\d{4}/\d{2}/\d{2}$' THEN TO_DATE(SCRAPED_DATE, 'YYYY/MM/DD')
      WHEN SCRAPED_DATE ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(SCRAPED_DATE, 'DD/MM/YYYY')
    END AS SCRAPED_DATE,
    ROOM_TYPE,
    ACCOMMODATES,
    PRICE,
    HAS_AVAILABILITY
FROM {{ source('raw', 'listings') }}
{% endsnapshot %}


