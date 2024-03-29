from sqlalchemy import create_engine, inspect
import pandas as pd
import yaml 


class DatabaseConnector:
    """
    Handles connectivity and interactions with databases.

        Args:
            db_creds_yaml: yaml file containing database credentials
    """
    def __init__(self, db_creds) -> None:
        self.db_creds = self.read_db_creds(db_creds)
        
    def read_db_creds(self, creds_file: str) -> dict:
        """
        Reads database credentials from db_creds_yaml and parses to a dictionary.

        Args:
            creds_file (str): path for file containing database credentials
        Returns:
            db_creds_dict: a dictionary containing database credentials
        """
        with open(creds_file, "r") as yf:
            db_creds_dict = yaml.safe_load(yf)
            yf.close()
        return db_creds_dict
    
    def init_db_engine(self):
        """
        Get database credentials using the read_db_creds() method and intializes
        a sqlalchemy database engine. Takes in an optional argument with a path to a file containing database credentials.
        If the argument is not passed the credentials are taken from the db_creds file as default.

        Returns:
            engine: an sqlalchemy database engine
        """
        RDS_DATABASE_TYPE = 'postgresql'
        RDS_DBAPI = 'psycopg2'
        RDS_HOST = self.db_creds.get("RDS_HOST")
        RDS_USER = self.db_creds.get("RDS_USER")
        RDS_PASSWORD = self.db_creds.get("RDS_PASSWORD")
        RDS_DATABASE = self.db_creds.get("RDS_DATABASE")
        RDS_PORT = self.db_creds.get("RDS_PORT")
        engine = create_engine(f"{RDS_DATABASE_TYPE}+{RDS_DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        return engine
    
    def list_db_tables(self) -> list:
        """
        Establishes an active connection to the database by using the engine returned from the init_db_engine method.
        The databse is inspected and names of tables returned with the use of the inspect() function and the following
        get_table_names() method.

        Returns:
            table_names: a list of table names, which are available in the database
        """
        db_engine = self.init_db_engine()
        with db_engine.execution_options(isolation_level='AUTOCOMMIT').connect():
            inspector = inspect(db_engine)
            table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Takes in a pandas dataframe and uploads the data to a local database. The new table name is passed in the table_name
        argument.

        Args:
            df (pd.DataFrame): a dataframe to be processed into an SQL table
            table_name (str): the name of the new table to be created
        """
        engine = self.init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect():
            df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f'Table {table_name} has been created.')
