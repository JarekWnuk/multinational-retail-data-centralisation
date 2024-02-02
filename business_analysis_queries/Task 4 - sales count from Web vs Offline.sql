-- Task 4
-- How many sales are coming from online?

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