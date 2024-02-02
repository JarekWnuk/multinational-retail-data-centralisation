-- Task 1
-- What is the maximum length of the country_code?
SELECT MAX(LENGTH(country_code))
FROM
	dim_user_details;

-- Cast the columns to the correct data types:
ALTER TABLE dim_user_details
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN join_date TYPE DATE;