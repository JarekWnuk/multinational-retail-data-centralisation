-- Task 2
--Which locations currently have the most stores?

SELECT locality, COUNT(locality) AS total_no_stores
FROM
	dim_store_details
GROUP BY
	locality
ORDER BY
	COUNT(locality) DESC
LIMIT 7;