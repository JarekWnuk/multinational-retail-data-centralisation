-- Task 5
-- Rename column:
ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;

-- Cast the columns to the correct data types:
ALTER TABLE dim_products
	ALTER COLUMN product_price TYPE FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ADD PRIMARY KEY product_code,
	ALTER COLUMN date_added TYPE DATE,
	ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
	ALTER COLUMN still_available TYPE BOOL USING
	CASE
		WHEN still_available='Removed' THEN FALSE
		ELSE TRUE
	END;