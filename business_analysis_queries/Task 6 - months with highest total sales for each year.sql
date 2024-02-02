-- Task 6
-- Which month in each year produced the highest sales?

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