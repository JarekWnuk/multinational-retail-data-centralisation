-- Task 1
SELECT country_code AS country, COUNT(country_code) AS total_no_of_stores
FROM
	dim_store_details
GROUP BY
	country_code
ORDER BY
	total_no_of_stores DESC;

-- Task 2
SELECT locality, COUNT(locality) AS total_no_stores
FROM
	dim_store_details
GROUP BY
	locality
ORDER BY
	COUNT(locality) DESC
LIMIT 7;

-- Task 3	
SELECT SUM(orders_table.product_quantity * ROUND(p.product_price::numeric, 2)) AS total_sales, EXTRACT(MONTH FROM (d.date_time)) AS "month"
FROM
	orders_table
INNER JOIN dim_products AS p
	ON orders_table.product_code = p.product_code
INNER JOIN dim_date_times AS d
	ON orders_table.date_uuid = d.date_uuid
INNER JOIN dim_store_details AS s
	ON orders_table.store_code = s.store_code
GROUP BY
	EXTRACT(MONTH FROM (d.date_time))
ORDER BY
	total_sales DESC
LIMIT 6;

-- Task 4
SELECT COUNT(product_quantity) AS number_of_sales, SUM(product_quantity) AS product_quantity_count,
CASE
	WHEN sd.store_type = 'Web Portal' THEN 'Web'
	ELSE 'Offline'
END AS "location"
FROM 
	orders_table
INNER JOIN dim_store_details AS sd
	ON orders_table.store_code = sd.store_code
GROUP BY
	"location"
ORDER BY
	COUNT(product_quantity);
	
-- Task 5
WITH my_cte AS (
	SELECT SUM(ROUND(product_price::numeric,2) * product_quantity) AS total_sales_all
	FROM dim_products AS p
	INNER JOIN orders_table AS ot
		ON p.product_code = ot.product_code)

SELECT store_type, SUM(ROUND(product_price::numeric,2) * product_quantity) AS total_sales,
	ROUND(SUM(ROUND(product_price::numeric,2) * product_quantity) /
	(SELECT SUM(ROUND(product_price::numeric,2) * product_quantity)
	 FROM
		dim_store_details AS sd
	INNER JOIN orders_table AS ot
		ON sd.store_code = ot.store_code
	INNER JOIN dim_products AS p
		ON ot.product_code = p.product_code ) * 100,2) AS percentage
FROM
	dim_store_details AS sd
INNER JOIN orders_table AS ot
	ON sd.store_code = ot.store_code
INNER JOIN dim_products AS p
	ON ot.product_code = p.product_code
GROUP BY
	store_type
ORDER BY
	total_sales DESC;
	
-- Task 6
SELECT 
	SUM(ROUND(product_price::numeric,2) * product_quantity) AS total_sales,
	EXTRACT(YEAR FROM date_time) AS "Year",
	EXTRACT(MONTH FROM date_time) AS "Month"
FROM
	orders_table AS ot
INNER JOIN dim_products AS p
	ON ot.product_code = p.product_code
INNER JOIN dim_date_times AS dt
	ON ot.date_uuid = dt.date_uuid
GROUP BY
	EXTRACT(MONTH FROM date_time),
	EXTRACT(YEAR FROM date_time)
ORDER BY
	total_sales DESC
LIMIT
	10;
	
-- Task 7
SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
FROM
	dim_store_details
GROUP BY
	country_code
ORDER BY
	total_staff_numbers DESC;
	
-- Task 8
SELECT	
	SUM(ROUND(product_price::numeric,2) * product_quantity) AS total_sales,
	store_type,
	country_code
FROM
	orders_table AS ot
INNER JOIN dim_products AS p
	ON ot.product_code = p.product_code
INNER JOIN dim_store_details AS sd
	ON ot.store_code = sd.store_code
WHERE
	country_code = 'DE'
GROUP BY
	store_type,
	country_code
ORDER BY total_sales;

-- Task 9
WITH cte AS(
SELECT
	date_time,
	EXTRACT(YEAR FROM date_time) AS "year",
	date_time AS "current_date_time",
	LEAD(date_time) OVER(ORDER BY date_time) AS next_time
FROM 
	dim_date_times
),
cte2 AS(
SELECT
	date_time,
	year,
	next_time - "current_date_time" AS time_between_sales
FROM
	cte
)
SELECT
	year,
	CONCAT(' hours: ', CAST(EXTRACT( HOUR FROM AVG(time_between_sales)) AS TEXT),
		  ' minutes: ', CAST(EXTRACT( MINUTE FROM AVG(time_between_sales)) AS TEXT),
		   ' seconds: ',CAST(EXTRACT( SECOND FROM AVG(time_between_sales)) AS TEXT)) AS actual_time_taken
FROM 
	cte2
GROUP BY
	year
ORDER BY
	AVG(time_between_sales) DESC
LIMIT 5;