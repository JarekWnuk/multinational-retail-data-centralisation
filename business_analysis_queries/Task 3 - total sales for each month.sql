-- Task 3
-- Which months produced the largest amount of sales?

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