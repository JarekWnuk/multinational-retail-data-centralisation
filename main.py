import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

new_database_conn = DatabaseConnector()
new_data_extractor = DataExtractor()
data_cleaning = DataCleaning()

# process user data
df_user_data = new_data_extractor.read_rds_table(new_database_conn, 'legacy_users')
df_user_data_clean = data_cleaning.clean_user_data(df_user_data)
new_database_conn.upload_to_db(df_user_data_clean, 'dim_user_details')

# process card data
df_card_details_from_pdf = new_data_extractor.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')    
df_card_details_clean = data_cleaning.clean_card_data(df_card_details_from_pdf)
new_database_conn.upload_to_db(df_card_details_clean, 'dim_card_details')

# process stores data
st_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
header_dict = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
st_data_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
no_of_stores = new_data_extractor.list_number_of_stores(st_endpoint, header_dict)
df_stores = new_data_extractor.retrieve_stores_data(st_data_endpoint, no_of_stores, header_dict)
df_stores_clean = data_cleaning.clean_store_data(df_stores)
new_database_conn.upload_to_db(df_stores_clean, 'dim_store_details')

# process products data
df_products_to_clean = new_data_extractor.extract_from_s3('s3://data-handling-public/products.csv')
df_clean_product_weights = data_cleaning.convert_product_weights(df_products_to_clean)
df_products_clean = data_cleaning.clean_products_data(df_clean_product_weights)
new_database_conn.upload_to_db(df_products_clean, 'dim_products')

# process orders data
df_orders_to_clean = new_data_extractor.read_rds_table(new_database_conn, 'orders_table')
df_orders_clean = data_cleaning.clean_orders_data(df_orders_to_clean)
new_database_conn.upload_to_db(df_orders_clean, 'orders_table')

# process date times data 
df_date_times_to_clean = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
df_date_times_clean = data_cleaning.clean_date_times(df_date_times_to_clean)
new_database_conn.upload_to_db(df_date_times_clean, 'dim_date_times')