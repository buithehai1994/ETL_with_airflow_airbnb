WITH source AS (
    SELECT * FROM {{ ref('facts_listings') }}
)

SELECT
    lga_name AS host_neighbourhood_lga,
    TO_CHAR(scraped_date, 'Mon/YYYY') AS month_year,
    COUNT(DISTINCT host_id) AS distinct_hosts,
    SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END) AS estimated_revenue,
    SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END) / NULLIF(COUNT(DISTINCT host_id), 0) AS avg_estimated_revenue_per_host
FROM source
GROUP BY lga_name, month_year
ORDER BY host_neighbourhood_lga, month_year




