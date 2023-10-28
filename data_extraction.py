import yaml
from sqlalchemy import create_engine

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

new_data_extractor = DataExtractor()
db_credentials_dict = new_data_extractor.read_db_creds()
