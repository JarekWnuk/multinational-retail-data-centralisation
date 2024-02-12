from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from dotenv import load_dotenv
import os
import pandas as pd


def process_user_data() -> None:
    """
    Retrieves, cleans and uploads data for users.
    """
    df_user_data = new_data_extractor.read_rds_table(remote_database_conn, 'legacy_users')
    df_user_data_clean = data_cleaning.clean_user_data(df_user_data)
    local_database_conn.upload_to_db(df_user_data_clean, 'dim_user_details')

def process_card_data(card_details_endpoint: str) -> None:
    """
    Retrieves, cleans and uploads data for card details.

        Args:
            card_datails_endpoint (str): S3 endpoint for the pdf file containing card details data,
    """
    df_card_details_from_pdf = new_data_extractor.retrieve_pdf_data(card_details_endpoint)    
    df_card_details_clean = data_cleaning.clean_card_data(df_card_details_from_pdf)
    local_database_conn.upload_to_db(df_card_details_clean, 'dim_card_details')

def process_stores_data(st_endpoint: str, st_data_endpoint: str, api_key: str ) -> None:
    """
    Retrieves, cleans and uploads data for stores.
    
        Args:
            st_endpoint (str): API end point for the total number of stores
            st_data_endpoint (str) : API endpoint for store details data
            x_api_key (str): API key
    """
    header_dict = {"x-api-key":api_key}
    no_of_stores = new_data_extractor.list_number_of_stores(st_endpoint, header_dict)
    df_stores = new_data_extractor.retrieve_stores_data(st_data_endpoint, no_of_stores, header_dict)
    df_stores_clean = data_cleaning.clean_store_data(df_stores)
    local_database_conn.upload_to_db(df_stores_clean, 'dim_store_details')

def process_products_data(products_endpoint: str) -> None:
    """
    Retrieves, cleans and uploads data for products.
    
        Args:
            products_endpoint (str): S3 endpoint for file containing data for products, works for csv and json files
    """
    df_products_to_clean = new_data_extractor.extract_from_s3(products_endpoint)
    df_clean_product_weights = data_cleaning.convert_product_weights(df_products_to_clean)
    df_products_clean = data_cleaning.clean_products_data(df_clean_product_weights)
    local_database_conn.upload_to_db(df_products_clean, 'dim_products')

def process_orders_data():
    """
    Retrieves, cleans and uploads data for orders.
    """
    df_orders_to_clean = new_data_extractor.read_rds_table(remote_database_conn, 'orders_table')
    df_orders_clean = data_cleaning.clean_orders_data(df_orders_to_clean)
    local_database_conn.upload_to_db(df_orders_clean, 'orders_table')

def process_date_times_data(date_times_endpoint: str) -> None:
    """
    Retrieves, cleans and uploads data for date times
    
        Args:
            date_times_endpoint (str): S3 endpoint for file containing date time data, works with json files
    """
    df_date_times_to_clean = pd.read_json(date_times_endpoint)
    df_date_times_clean = data_cleaning.clean_date_times(df_date_times_to_clean)
    local_database_conn.upload_to_db(df_date_times_clean, 'dim_date_times')


if __name__ == "__main__":

    local_database_conn = DatabaseConnector('db_creds_local.yaml')  # file with credentials for local databse is passed to class instance
    remote_database_conn = DatabaseConnector('db_creds.yaml')       # file with credentials for remote databse is passed to class instance
    new_data_extractor = DataExtractor()
    data_cleaning = DataCleaning()

    process_user_data()

    card_details_endpoint = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    process_card_data(card_details_endpoint)

    st_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    st_data_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
    load_dotenv('x_api_key.env')
    api_key = os.getenv('x-api-key')
    process_stores_data(st_endpoint, st_data_endpoint, api_key)

    products_endpoint = 's3://data-handling-public/products.csv'
    process_products_data(products_endpoint)

    date_times_endpoint = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    process_date_times_data(date_times_endpoint)