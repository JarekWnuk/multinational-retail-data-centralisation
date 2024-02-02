-- Task 5
-- What percentage of sales comes from each type of store?

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