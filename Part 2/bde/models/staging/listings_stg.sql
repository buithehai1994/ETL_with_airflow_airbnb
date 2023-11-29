-- Common Table Expression (CTE) to select data from the "listings" table in the "raw" schema.
WITH dat AS (
    SELECT
        {{ source('raw', 'listings') }}.*,
        CASE
            WHEN HOST_SINCE ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(HOST_SINCE, 'DD/MM/YYYY')
            WHEN HOST_SINCE ~ '^\d{1}/\d{2}/\d{4}$' THEN TO_DATE(HOST_SINCE, 'D/MM/YYYY')
            WHEN HOST_SINCE ~ '^\d{2}/\d{1}/\d{4}$' THEN TO_DATE(HOST_SINCE, 'DD/M/YYYY')
            WHEN HOST_SINCE ~ '^\d{1}/\d{1}/\d{4}$' THEN TO_DATE(HOST_SINCE, 'D/M/YYYY')
            WHEN NOT HOST_SINCE ~ '^[0-9]+$' THEN NULL
            ELSE TO_DATE(NULL, 'DD/MM/YYYY') -- Replaces 'NaN' with NULL for date columns
        END AS HOST_SINCE_PROCESSED,
        CASE
            WHEN SCRAPED_DATE ~ '^\d{4}-\d{2}-\d{2}$' THEN TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD')
            WHEN SCRAPED_DATE ~ '^\d{4}/\d{2}/\d{2}$' THEN TO_DATE(SCRAPED_DATE, 'YYYY/MM/DD')
            WHEN SCRAPED_DATE ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(SCRAPED_DATE, 'DD/MM/YYYY')
            ELSE TO_DATE(NULL, 'YYYY-MM-DD') -- Replaces 'NaN' with NULL for date columns
        END AS SCRAPED_DATE_PROCESSED,
        CASE
            WHEN HOST_NEIGHBOURHOOD = 'NaN' OR HOST_NEIGHBOURHOOD IS NULL THEN 'Unknown'
            ELSE HOST_NEIGHBOURHOOD
        END AS HOST_NEIGHBOURHOOD_PROCESSED
    FROM {{ source('raw', 'listings') }}
)

-- Calculate modes for review score columns
, median_values AS (
    SELECT
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_accuracy, 'NaN')) AS median_review_scores_accuracy,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_cleanliness, 'NaN')) AS median_review_scores_cleanliness,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_checkin, 'NaN')) AS median_review_scores_checkin,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_communication, 'NaN')) AS median_review_scores_communication,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_value, 'NaN')) AS median_review_scores_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_rating, 'NaN')) AS median_review_scores_rating
    FROM dat
    WHERE
        review_scores_accuracy BETWEEN 1 AND 10
        AND review_scores_cleanliness BETWEEN 1 AND 10
        AND review_scores_checkin BETWEEN 1 AND 10
        AND review_scores_communication BETWEEN 1 AND 10
        AND review_scores_value BETWEEN 1 AND 10
        AND review_scores_rating BETWEEN 1 AND 100
),
median_neighbourhood AS (
    SELECT
        listing_neighbourhood,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_accuracy, 'NaN')) AS median_neighbourhood_avg_accuracy,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_cleanliness, 'NaN')) AS median_neighbourhood_avg_cleanliness,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_checkin, 'NaN')) AS median_neighbourhood_avg_checkin,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_communication, 'NaN')) AS median_neighbourhood_avg_communication,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 
            NULLIF(review_scores_value, 'NaN')) AS median_neighbourhood_avg_value
    FROM dat
    WHERE
        review_scores_accuracy BETWEEN 1 AND 10
        AND review_scores_cleanliness BETWEEN 1 AND 10
        AND review_scores_checkin BETWEEN 1 AND 10
        AND review_scores_communication BETWEEN 1 AND 10
        AND review_scores_value BETWEEN 1 AND 10
    GROUP BY listing_neighbourhood
)


-- Main Query:
SELECT
    dat.listing_id,
    dat.scrape_id,
    dat.SCRAPED_DATE_PROCESSED AS scraped_date,
    dat.host_id,
    dat.host_name,
    dat.HOST_SINCE_PROCESSED AS host_since,
    dat.host_is_superhost,
    dat.HOST_NEIGHBOURHOOD_PROCESSED AS host_neighbourhood,
    dat.listing_neighbourhood,
    dat.property_type,
    dat.room_type,
    dat.accommodates,
    dat.price,
    dat.has_availability,
    dat.availability_30,
    dat.number_of_reviews,
    COALESCE(NULLIF(dat.review_scores_rating, 'NaN'), median_values.median_review_scores_rating) AS review_scores_rating,
    COALESCE(NULLIF(dat.review_scores_accuracy, 'NaN'), median_values.median_review_scores_accuracy) AS review_scores_accuracy,
    COALESCE(NULLIF(dat.review_scores_cleanliness, 'NaN'), median_values.median_review_scores_cleanliness) AS review_scores_cleanliness,
    COALESCE(NULLIF(dat.review_scores_checkin, 'NaN'), median_values.median_review_scores_checkin) AS review_scores_checkin,
    COALESCE(NULLIF(dat.review_scores_communication, 'NaN'), median_values.median_review_scores_communication) AS review_scores_communication,
    COALESCE(NULLIF(dat.review_scores_value, 'NaN'), median_values.median_review_scores_value) AS review_scores_value
FROM dat
CROSS JOIN median_values
WHERE dat.host_since IS NOT NULL AND dat.host_since != 'null' AND dat.host_is_superhost != 'NaN' AND dat.number_of_reviews > 0
