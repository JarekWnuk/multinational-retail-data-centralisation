-- Task 8
-- Which German store type is selling the most?

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