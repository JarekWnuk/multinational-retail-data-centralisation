import yaml
from sqlalchemy import create_engine, inspect, text

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

        Returns:
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

        Returns:
            table_names: a list of table names, which are present in the database
        """
        db_engine = self.init_db_engine()
        with db_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            inspector = inspect(db_engine)
            table_names = inspector.get_table_names()
        conn.close()
        return table_names
    
    def query_db(self, query: str):
        """
        Establishes an active connection to the database by using the engine returned from the init_db_engine method.
        Takes in a query that is executed on the connected database.

        Args:
            query (str): SQL query to be executed on the database
        Returns:
            result: the result of the query, which is an iterable ResultProxy object
        """
        db_engine = self.init_db_engine()
        with db_engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            result = conn.execute(text(query))
        conn.close()
        return result


new_data_extractor = DataExtractor()
query = "SELECT * FROM orders_table LIMIT 10"
query_result = new_data_extractor.query_db(query)
for row in query_result:
    print(row)


