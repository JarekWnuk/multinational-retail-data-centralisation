-- Task 9
-- How quickly is the company making sales?

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