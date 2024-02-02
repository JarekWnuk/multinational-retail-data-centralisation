-- Task 1
-- How many stores does the business have and in which countries?

SELECT country_code AS country, COUNT(country_code) AS total_no_of_stores
FROM
	dim_store_details
GROUP BY
	country_code
ORDER BY
	total_no_of_stores DESC;