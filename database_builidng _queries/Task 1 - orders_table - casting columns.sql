-- Task 1
-- What is the maximum length of the card_number?
SELECT MAX(LENGTH(card_number))
FROM
	orders_table;

-- What is the maximum length of the store_code?
SELECT MAX(LENGTH(store_code))
FROM
	orders_table;
	
-- What is the maximum length of the product_code?
SELECT MAX(LENGTH(product_code))
FROM
	orders_table;

-- Cast the columns to the correct data types:
ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid,
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::uuid,
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT;
