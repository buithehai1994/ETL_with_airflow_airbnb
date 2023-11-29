WITH source AS (
    SELECT * FROM {{ ref('facts_listings') }}
)

SELECT
    listing_neighbourhood,
    TO_CHAR(scraped_date, 'Mon/YYYY') AS month_year,
    COUNT(*) AS total_listings,
    SUM(CASE WHEN has_availability = 't' THEN 1 ELSE 0 END) AS active_listings,
    MIN(price) AS min_price_active,
    MAX(price) AS max_price_active,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price_active,
    AVG(price) AS avg_price_active,
    COUNT(DISTINCT host_id) AS distinct_hosts,
    (COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id END) / COUNT(DISTINCT host_id) * 100) AS superhost_rate,
    AVG(CASE WHEN has_availability = 't' THEN review_scores_rating ELSE NULL END) AS avg_rating_active,
    ((COUNT(*) - COUNT(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END)) / COUNT(*) * 100) AS percentage_change_active,
    (COUNT(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END) / COUNT(*) * 100) AS percentage_change_inactive,
    (COUNT(CASE WHEN has_availability = 't' THEN 30 - availability_30 ELSE NULL END)) AS total_stays,
    SUM(CASE WHEN has_availability = 't' THEN (30 - availability_30) * price ELSE 0 END) / NULLIF(COUNT(CASE WHEN has_availability = 't' THEN 1 ELSE NULL END), 0) AS avg_estimated_revenue_per_active_listing
FROM source
GROUP BY listing_neighbourhood, month_year
ORDER BY listing_neighbourhood, month_year

