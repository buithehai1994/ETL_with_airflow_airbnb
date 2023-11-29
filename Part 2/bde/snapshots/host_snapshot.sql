-- Snapshot for host_snapshot
{% snapshot host_snapshot %}

{{
    config(
        target_schema='raw',
        materialized='snapshot',
        unique_key='id',
        strategy='timestamp',
        updated_at='SCRAPED_DATE'
    )
}}
-- Select and populate the snapshot with data from the 'listing' source table
SELECT distinct
    MD5(HOST_NAME) as id,
    HOST_NAME,
    CASE
      WHEN host_since ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(host_since, 'DD/MM/YYYY')
      WHEN host_since ~ '^\d{1}/\d{2}/\d{4}$' THEN TO_DATE(host_since, 'D/MM/YYYY')
      WHEN host_since ~ '^\d{2}/\d{1}/\d{4}$' THEN TO_DATE(host_since, 'DD/M/YYYY')
      WHEN host_since ~ '^\d{1}/\d{1}/\d{4}$' THEN TO_DATE(host_since, 'D/M/YYYY')
      WHEN host_since = 'NaN' THEN NULL
    END AS host_since,
    CASE
      WHEN SCRAPED_DATE ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD')
      WHEN SCRAPED_DATE ~ '^\d{4}/\d{2}/\d{2}$' THEN TO_DATE(SCRAPED_DATE, 'YYYY/MM/DD')
      WHEN SCRAPED_DATE ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(SCRAPED_DATE, 'DD/MM/YYYY')
    END AS SCRAPED_DATE,
    HOST_IS_SUPERHOST,
    HOST_NEIGHBOURHOOD
FROM {{ source('raw', 'listings') }}
{% endsnapshot %}
