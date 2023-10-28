import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect

class DataExtractor:
    def __init__(self) -> None:
        pass
    
    def read_db_creds(self) -> dict:
        """
        Reads database credentials from db_creds_yaml and parses to a dictionary.

        Returns:
            db_creds_dict: a dictionary containing database credentials
        """
        with open("db_creds.yaml", "r") as yf:
            db_creds_dict = yaml.safe_load(yf)
            yf.close()
        return db_creds_dict
    
    def init_db_engine(self):
        """
        Get database credentials using the read_db_creds() method and intializes
        a sqlalchemy database engine.

        Returns
            engine: an sqlalchemy database engine
        """
        db_creds = self.read_db_creds()
        RDS_DATABASE_TYPE = 'postgresql'
        RDS_DBAPI = 'psycopg2'
        RDS_HOST = db_creds.get("RDS_HOST")
        RDS_USER = db_creds.get("RDS_USER")
        RDS_PASSWORD = db_creds.get("RDS_PASSWORD")
        RDS_DATABASE = db_creds.get("RDS_DATABASE")
        RDS_PORT = db_creds.get("RDS_PORT")
        engine = create_engine(f"{RDS_DATABASE_TYPE}+{RDS_DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        return engine
    
    def list_db_tables(self) -> list:
        """
        Establishes an active connection to the database by using the engine returned from the init_db_engine method.
        The databse is inspected and names of tables returned with the use of the inspect() function and the following
        get_table_names() method.

        Returns
            table_names: a list of table names, which are present in the database
        """
        db_engine = self.init_db_engine()
        with db_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            inspector = inspect(db_engine)
            table_names = inspector.get_table_names()
        conn.close()
        return table_names

new_data_extractor = DataExtractor()
print(new_data_extractor.list_db_tables())
