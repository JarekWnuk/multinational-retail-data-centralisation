-- Task 7
-- What is the maximum length of card_number?
SELECT MAX(LENGTH(card_number))
FROM
	dim_card_details;

-- What is the maximum length of card_provider?
SELECT MAX(LENGTH(card_provider))
FROM
	dim_card_details;
	
-- The expiry_date was inferred as the last day of the corresponding month during data cleaning and will be cast to DATE.

-- Cast the columns to the correct data types:
ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN card_provider TYPE VARCHAR(27),
	ALTER COLUMN expiry_date TYPE DATE USING expiry_date::date,
	ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;