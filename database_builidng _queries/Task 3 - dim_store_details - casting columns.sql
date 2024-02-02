-- Task 3
-- Cast the columns to the correct data types:
ALTER TABLE dim_store_details 
	ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT,
	ALTER COLUMN opening_date TYPE DATE,
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN store_type DROP NOT NULL,
	ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255);

-- Update the address and locality of the business's website from N/A to NULL:
UPDATE dim_store_details
	SET address = 'N/A' 
	WHERE address IS NULL;
	
UPDATE dim_store_details
	SET	locality = 'N/A' 
	WHERE locality IS NULL;
	
