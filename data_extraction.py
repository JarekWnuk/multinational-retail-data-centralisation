from sqlalchemy import text

class DataExtractor:
    def __init__(self) -> None:
        pass
    
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


