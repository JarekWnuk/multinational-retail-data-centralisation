import pandas as pd
import numpy as np
from sqlalchemy import text
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from dateutil.parser import parse

class DataExtractor:
    def __init__(self) -> None:
        pass
    
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
        conn.close()
        return result
    
    def read_rds_table(self, db_connector_instance: DatabaseConnector, table_name:str) -> pd.DataFrame:
        """
        Extracts all data from the passed table (if it exists) and parses to a pandas DataFrame.

        Args:
            db_connector_instance (DatabaseConnector): an instance of the DatabaseConnector class
            table_name (str): the name of the table from which to extract data
        """
        if table_name in db_connector_instance.list_db_tables():
            query = f"SELECT * FROM {table_name}"
            query_result = self.query_db(db_connector_instance, query)
            df = pd.DataFrame(query_result)
        else:
            print("The table specified does not exist.")
        return df

new_database_conn = DatabaseConnector()
new_data_extractor = DataExtractor()
data_cleaning = DataCleaning()
user_data_df = new_data_extractor.read_rds_table(new_database_conn, 'legacy_users')
user_data_df_clean = data_cleaning.clean_user_data(user_data_df)
print(user_data_df_clean.dtypes)





