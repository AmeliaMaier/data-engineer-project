from ConnectionBase import ConnectionBase
import psycopg2
import psycopg2.extras
import pandas as pd
import pandas.io.sql as psql

class PSQLUtil(ConnectionBase):
    """
        An implementation of connection for psql as you would never 
        actually transform source csvs just to save them as csvs and 
        then import them into a database manually.
    """
    def __init__(self, conn):
        self.conn = conn
        self.curser = conn.cursor()

    def table_exists(self, schema, table_name):
        """
        If the system talks to multiple schemas, the schema would need to be passed in
        (and the user_name for the connection would need to have access to all of them).
        Otherwise, it would be better for it to be an environment variable.
        Parameters
        ----------
        schema : str
            The schema the table should be in.
        table_name : str
            The table name to search for.
        """
        query ="""SELECT EXISTS ( 
                        SELECT 1
                        FROM   information_schema.tables 
                        WHERE  table_schema = 'schema_name'
                        AND    table_name = 'table_name'
                        );"""
        return self._query_to_df(query)[0]

    def _query_to_df(self, query_str):
        df = pd.read_sql_query(query_str, self.conn)
        return df

    def _simple_execute(self, query_str):
        self.curser.execute(query_str)
        self.conn.commit()

    def __del__(self):
        self.conn.commit()
        self.conn.close()