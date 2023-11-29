--a.	What are the main differences from a population point of view (i.g. higher population of under 30s) 
--between the best performing “listing_neighbourhood” and the worst (in terms of estimated revenue per 
--active listings) over the last 12 months? 

WITH RevenueSummary AS (
    SELECT
        listing_neighbourhood,
        AVG(price) AS avg_price,
        COUNT(*) AS active_listings,
        SUM(price) AS total_revenue
    FROM
        warehouse.facts_listings
    WHERE
        has_availability = 't' -- Filter for active listings
    GROUP BY
        listing_neighbourhood
),
RankedNeighborhoods AS (
    SELECT
        listing_neighbourhood,
        avg_price,
        active_listings,
        total_revenue,
        RANK() OVER (ORDER BY total_revenue DESC) AS best_rank,
        RANK() OVER (ORDER BY total_revenue ASC) AS worst_rank
    FROM
        RevenueSummary
)
SELECT
    'Best Performing' AS performance_category,
    RN.listing_neighbourhood,
    RN.avg_price AS best_avg_price,
    RN.active_listings AS best_active_listings,
    RN.total_revenue AS best_total_revenue
FROM
    RankedNeighborhoods RN
WHERE
    RN.best_rank = 1 
UNION ALL
SELECT
    'Worst Performing' AS performance_category,
    RN.listing_neighbourhood,
    RN.avg_price AS worst_avg_price,
    RN.active_listings AS worst_active_listings,
    RN.total_revenue AS worst_total_revenue
FROM
    RankedNeighborhoods RN
WHERE
    RN.worst_rank = 1;
   
-- b. What will be the best type of listing (property type, room type, and accommodates)
-- for the top 5 “listing_neighbourhoods”
-- (in terms of estimated revenue per active listing) to have the highest number of stays?

-- Top 5 “listing_neighbourhood” (in terms of estimated revenue per active listing) 
WITH EstimatedRevenue AS (
    WITH revenue_calc AS (
       SELECT
           listing_id,
           property_type,
           room_type,
           accommodates,
           (30 - availability_30) AS number_of_stays,
           price,
           (30 - availability_30) * price AS estimated_revenue,
           listing_neighbourhood
       FROM warehouse.facts_listings
       WHERE has_availability = 't'
    )
    SELECT
       listing_id,
       property_type,
       room_type,
       accommodates,
       price,
       listing_neighbourhood,
       SUM(estimated_revenue) as estimated_revenue,
       SUM(number_of_stays) as number_of_stays
    FROM revenue_calc
    GROUP BY listing_id, property_type, room_type, accommodates, price, listing_neighbourhood
),
TopNeighbourhoods AS (
    SELECT
        listing_neighbourhood,
        SUM(estimated_revenue) / COUNT(listing_id) as estimated_revenue_per_active_listing
    FROM EstimatedRevenue
    GROUP BY listing_neighbourhood
    ORDER BY estimated_revenue_per_active_listing DESC
    LIMIT 5
),
BestListingTypes AS (
    SELECT
        e.listing_neighbourhood,
        e.property_type,
        e.room_type,
        e.accommodates,
        SUM(e.number_of_stays) as total_stays
    FROM EstimatedRevenue e
    JOIN TopNeighbourhoods t ON e.listing_neighbourhood = t.listing_neighbourhood
    GROUP BY e.listing_neighbourhood, e.property_type, e.room_type, e.accommodates
    ORDER BY e.listing_neighbourhood, total_stays DESC
),
BestListingRank AS (
    SELECT
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        total_stays,
        ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood ORDER BY total_stays DESC) AS ranking
    FROM BestListingTypes
),
Top5Neighbourhoods AS (
    SELECT
        listing_neighbourhood
    FROM TopNeighbourhoods
),
EstimatedRevenueForTop5 AS (
    SELECT
        e.listing_neighbourhood,
        SUM(e.estimated_revenue) as total_estimated_revenue
    FROM EstimatedRevenue e
    WHERE e.listing_neighbourhood IN (SELECT listing_neighbourhood FROM Top5Neighbourhoods)
    GROUP BY e.listing_neighbourhood
)
SELECT
    b.listing_neighbourhood,
    b.property_type,
    b.room_type,
    b.accommodates,
    b.total_stays,
    (er.total_estimated_revenue / b.total_stays) AS estimated_revenue_per_active_listing
FROM BestListingRank b
JOIN EstimatedRevenueForTop5 er ON b.listing_neighbourhood = er.listing_neighbourhood
WHERE b.ranking = 1;

   
-- c.	Do hosts with multiple listings are more inclined to have their listings 
--in the same LGA as where they live?
    
WITH HostListings AS (
    SELECT
        host_id,
        listing_neighbourhood,
        host_neighbourhood_lga_name AS host_lga
    from warehouse.facts_listings
        
    WHERE
        has_availability = 't' -- Filter for active listings
),
HostsWithMultipleListings AS (
    SELECT
        host_id
    FROM
        HostListings
    GROUP BY
        host_id
    HAVING
        COUNT(DISTINCT listing_neighbourhood) > 1 -- Hosts with multiple listings in different neighborhoods
),
HostsInSameLGA AS (
    SELECT
        HL.host_id,
        COUNT(DISTINCT HL.listing_neighbourhood) AS num_neighborhoods,
        HL.host_lga
    FROM
        HostListings HL
    JOIN
        HostsWithMultipleListings HML
        ON HL.host_id = HML.host_id
    GROUP BY
        HL.host_id, HL.host_lga
)
SELECT
    CASE
        WHEN HISL.num_neighborhoods = 1 THEN 'Yes'
        ELSE 'No'
    END AS same_lga_as_home,
    COUNT(*) AS num_hosts
FROM
    HostsInSameLGA HISL
GROUP BY
    same_lga_as_home;

-- d.	For hosts with a unique listing, does their estimated revenue 
--over the last 12 months can cover the annualised median mortgage repayment of their listing’s “listing_neighbourhood”?

WITH dat AS (
    SELECT *
    FROM warehouse.facts_listings lst
    INNER JOIN warehouse.dim_g01 AS g01
        ON CAST(SUBSTRING(g01.lga_code, 4) AS INTEGER) = CAST(lst.listing_neighbourhood_lga_code AS INTEGER)
    INNER JOIN warehouse.dim_g02 AS g02
        ON CAST(SUBSTRING(g02.lga_code, 4) AS INTEGER) = CAST(lst.listing_neighbourhood_lga_code AS INTEGER)
),
HostsWithUniqueListings AS (
    SELECT
    host_id,
    listing_neighbourhood
	FROM
	    dat
	WHERE host_id IN 
	(
	  SELECT host_id
	  FROM warehouse.facts_listings lst
	  INNER JOIN warehouse.dim_g01 AS g01
	      ON CAST(SUBSTRING(g01.lga_code, 4) AS INTEGER) = CAST(lst.listing_neighbourhood_lga_code AS INTEGER)
	  INNER JOIN warehouse.dim_g02 AS g02
	      ON CAST(SUBSTRING(g02.lga_code, 4) AS INTEGER) = CAST(lst.listing_neighbourhood_lga_code AS INTEGER)
	  GROUP BY host_id
	  HAVING COUNT(DISTINCT listing_id) = 1
	)

),
ListingRevenue AS (
    SELECT
        h.host_id,
        h.listing_neighbourhood,
        SUM((30 - availability_30) * price) AS annual_revenue
    FROM
        HostsWithUniqueListings AS h
    INNER JOIN
        dat AS d ON h.host_id = d.host_id AND h.listing_neighbourhood = d.listing_neighbourhood
    GROUP BY
        h.host_id, h.listing_neighbourhood
),
NeighborhoodMortgage AS (
    SELECT
        listing_neighbourhood,
        AVG(Median_mortgage_repay_monthly * 12) AS annual_median_mortgage_repayment
    FROM
        dat
    GROUP BY
        listing_neighbourhood
)
select 
    lr.host_id,
    lr.listing_neighbourhood,
    lr.annual_revenue,
    nm.annual_median_mortgage_repayment,
    CASE
        WHEN lr.annual_revenue >= nm.annual_median_mortgage_repayment THEN 'Covered'
        ELSE 'Not Covered'
    END AS coverage_status
FROM
    ListingRevenue AS lr
LEFT JOIN
    NeighborhoodMortgage AS nm ON lr.listing_neighbourhood = nm.listing_neighbourhood