-- Snapshot for property_snapshot
{% snapshot property_snapshot %}
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
    MD5(PROPERTY_TYPE) as id,
    CASE
      WHEN SCRAPED_DATE ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD')
      WHEN SCRAPED_DATE ~ '^\d{4}/\d{2}/\d{2}$' THEN TO_DATE(SCRAPED_DATE, 'YYYY/MM/DD')
      WHEN SCRAPED_DATE ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(SCRAPED_DATE, 'DD/MM/YYYY')
    END AS SCRAPED_DATE,
    PROPERTY_TYPE
FROM {{ source('raw', 'listings') }}
{% endsnapshot %}
