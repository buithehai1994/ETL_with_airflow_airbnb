-- Populate the new column dbt_valid_to with values from the existing table
UPDATE raw.host_snapshot AS t1
SET dbt_valid_to = t2.next_date
FROM (
	WITH dat AS (
	SELECT id, scraped_date,
	LEAD(scraped_date) OVER (PARTITION BY id ORDER BY scraped_date) AS next_date
	FROM raw.host_snapshot)

	SELECT id, scraped_date, 
	       (scraped_date + INTERVAL '1 day') AS next_date
	FROM dat

) AS t2
WHERE t1.id = t2.id AND t1.scraped_date = t2.scraped_date;



-- Populate the new column dbt_valid_to with values from the existing table
UPDATE raw.property_snapshot AS t1
SET dbt_valid_to = t2.next_date
FROM (
	WITH dat AS (
	SELECT id, scraped_date,
	LEAD(scraped_date) OVER (PARTITION BY id ORDER BY scraped_date) AS next_date
	FROM raw.property_snapshot
	)
	
	SELECT id, scraped_date, (scraped_date + INTERVAL '1 day') AS next_date
	FROM dat

) AS t2
WHERE t1.id = t2.id AND t1.scraped_date = t2.scraped_date;


-- Populate the new column dbt_valid_to_2 with values from the existing table
UPDATE raw.room_snapshot AS t1
SET dbt_valid_to = t2.next_date
FROM (
	WITH dat AS (
		SELECT id, scraped_date,
		LEAD(scraped_date) OVER (PARTITION BY id ORDER BY scraped_date) AS next_date
		FROM raw.room_snapshot)

	SELECT id, scraped_date, (scraped_date + INTERVAL '1 day') AS next_date
	FROM dat
) AS t2
WHERE t1.id = t2.id AND t1.scraped_date = t2.scraped_date;

