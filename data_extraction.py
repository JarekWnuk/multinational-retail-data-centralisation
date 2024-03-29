from database_utils import DatabaseConnector
from sqlalchemy import text
import boto3
import os.path
import re
import requests
import tabula
import pandas as pd 


class DataExtractor:
    def query_db(self,db_connector_instance: DatabaseConnector, query: str):
        """
        Uses an instance of the DatabaseConnector class to establish a connection to the database.
        Executes a query on the database and returns the result.

        Args:
            db_connector_instance (DatabaseConnector): an instance of the DatabaseConnector class
            query (str): a query to be executed on the database
        Returns:
            result (ResultProxy): query result
        """
        db_engine = db_connector_instance.init_db_engine()
        with db_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            result = conn.execute(text(query))
        return result
    
    def read_rds_table(self, db_connector_instance: DatabaseConnector, table_name:str) -> pd.DataFrame:
        """
        Extracts all data from the passed table (if it exists) and parses to a pandas DataFrame.

        Args:
            db_connector_instance (DatabaseConnector): an instance of the DatabaseConnector class
            table_name (str): the name of the table from which to extract data
        Returns:
            df (pd.Dataframe): a dataframe containing all the data from the specified table
        """
        if table_name in db_connector_instance.list_db_tables():
            query = f"SELECT * FROM {table_name}"
            query_result = self.query_db(db_connector_instance, query)
            df = pd.DataFrame(query_result)
            return df
        else:
            print("The table specified does not exist.")
    
    def retrieve_pdf_data(self, dir:str) -> pd.DataFrame:
        """
        Uses the tabula library to extract data from a pdf file to a pandas dataframe.
        The index is not reset on each page, instead it is continuous.
        
        Args:
            dir (str): directory of the pdf file, can be remote
        Returns:
            concat_df (pd.Dataframe): all pages of the pdf file concatenated to one dataframe
        """
        list_of_dfs = tabula.read_pdf(dir, pages='all')
        concat_df = pd.concat(list_of_dfs, ignore_index=True)
        return concat_df
    
    def list_number_of_stores(self, no_stores_endpoint: str, header_dict: dict) -> int:
        
        """
        Retrieves the number of stores from the passed endpoint. API credentials are passed as a dictionary.

        Args:
            no_stores_endpoint (str): the endpoint URL for retrieving the number of stores
            header_dict (dict): dictionary containing the authorization header with the x-api-key
        Returns:
            number_of_stores (int): the number of stores retrieved from the API
        """
        response = requests.get(no_stores_endpoint, headers=header_dict)
        data = response.json()
        return data['number_stores']
    
    def retrieve_stores_data(self, stores_data_endpoint: str, no_of_stores: int,  header_dict: dict) -> pd.DataFrame:
        """
        Retrieves data for all stores using an API. Converts each store data point to a dataframe and stores in a list.
        The list of dataframes is concatenated to one dataframe and saved to a CSV file before it is returned. The process of
        collecting all store data is time consuming and therefore it is more convenient to keep the data localy for further processing.       
        
        Args:
            stores_data_endpoint (str): the endpoint URL for retrieving store data
            no_of_stores (int): the total number of stores to extract
            header_dict (dict): dictionary containing the authorization header with the x-api-key
        Returns:
            df (pd.Dataframe): a dataframe containing combined data for all stores
        """
        # Checks if the file "df_stores.csv" has already been downloaded and exists in the local directory to avoid repeated downloads.
        if os.path.isfile('df_stores.csv'):
            with open('df_stores.csv', 'r') as f:
                df_store_data = pd.read_csv(f, index_col='index')
                f.close()
        else:
            # Retrieves the dataframe from the API for each store and adds to list - takes long to complete.
            df_list = []
            for store_number in range(0, no_of_stores):
                stores_data_endpoint_n = stores_data_endpoint +str(store_number)
                response = requests.get(stores_data_endpoint_n, headers=header_dict)
                data = response.json()
                df = pd.DataFrame([data])
                df_list.append(df)
                
            # Concatenates the list of dataframes and converts to csv for handling optimization
            df_store_data = pd.concat(df_list, ignore_index=True)
            df_store_data.to_csv('df_stores.csv', index=False)
        return df_store_data
    
    def extract_from_s3(self, address: str) -> pd.DataFrame:
        """
        Extracts a csv or json file from AWS s3 bucket.

        Args:
            address (str): address of the resuorce on s3
        Returns:
            pd.DataFrame: pandas dataframe created from the downloaded file
        """
        # Takes in the address and splits it to: bucket, key and filename.
        address_split = re.split('://|/', address)
        s3 = boto3.client('s3')
        bucket = address_split[1]
        key = address_split[2]
        filename = address_split[2]
        s3.download_file(Bucket=bucket, Key=key, Filename=filename)

        # Checks if the file is csv or json and converts to dataframe.
        with open(filename, 'r') as f:
            if ".csv" in filename:
                df_products = pd.read_csv(filename, index_col=0)
                f.close()
            elif ".json" in filename:
                df_products = pd.read_json(filename)
                f.close()
        return df_products



