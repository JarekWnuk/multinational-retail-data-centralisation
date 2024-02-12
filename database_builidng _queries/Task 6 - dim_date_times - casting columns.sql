-- Task 6
-- Columns "month", "year", "day" and timestamp have been combined to one column "date_time" during data cleaning.
-- Instead of casting to VARCHAR the column will be cast to DATE.

-- What is the maximum length of time_period?
SELECT MAX(LENGTH(time_period))
FROM
	dim_date_times;

-- Cast the columns to the correct data types:
ALTER TABLE dim_date_times
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;