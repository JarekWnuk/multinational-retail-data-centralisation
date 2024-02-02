-- Task 4
-- Add weight_class column:
ALTER TABLE dim_products
	ADD COLUMN weight_class VARCHAR(14);

-- The "£" character has already been removed from the product_price column during data cleaning.

-- Update weight_class column with weight classifications:
UPDATE dim_products
	SET weight_class = (CASE
		WHEN weight < 2 THEN 'Light'
		WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
		WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
		WHEN weight >=140 THEN 'Truck_Required'
	END);

