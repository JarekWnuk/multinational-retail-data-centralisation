ALTER TABLE orders_table
	ADD FOREIGN KEY (user_uuid) REFERENCES dim_user_details (user_uuid),
	ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
	ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
	ADD FOREIGN KEY (product_code) REFERENCES dim_products (product_code),
	ADD FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number);