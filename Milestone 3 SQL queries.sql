ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT;	
	ADD FOREIGN KEY (user_uuid) REFERENCES dim_user_details,
	ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
	ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
	ADD FOREIGN KEY (product_code) REFERENCES dim_products,
	ADD FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);

ALTER TABLE dim_user_details
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE DATE,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ADD PRIMARY KEY (user_uuid),
	ALTER COLUMN join_date TYPE DATE;
	
ALTER TABLE dim_store_details 
	ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ADD PRIMARY KEY store_code,
	ALTER COLUMN staff_numbers TYPE SMALLINT,
	ALTER COLUMN opening_date TYPE DATE,
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN store_type DROP NOT NULL,
	ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision,
	ALTER COLUMN country_code TYPE VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255);

UPDATE dim_store_details
	SET address = 'N/A' 
	WHERE address IS NULL;
	
UPDATE dim_store_details
	SET	locality = 'N/A' 
	WHERE locality IS NULL;
	
ALTER TABLE dim_products
	ADD COLUMN weight_class VARCHAR(14);
	
UPDATE dim_products
	SET weight_class = (CASE
		WHEN weight < 2 THEN 'Light'
		WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
		WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
		WHEN weight >=140 THEN 'Truck_Required'
	END);
	
ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;

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

ALTER TABLE dim_date_times
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_time TYPE DATE USING date_time::date,
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
	ADD PRIMARY KEY date_uuid;

ALTER TABLE dim_card_details
	ADD PRIMARY KEY (card_number),
	ALTER COLUMN card_number TYPE VARCHAR(22),
	ALTER COLUMN card_provider TYPE VARCHAR(27),
	ALTER COLUMN expiry_date TYPE DATE USING expiry_date::date,
	ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;


