from ConnectionBase import ConnectionBase
import psycopg2
import psycopg2.extras
import pandas as pd
import pandas.io.sql as psql

class PSQLConnection(ConnectionBase):
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
                        WHERE  table_schema = '%(schema)s'
                        AND    table_name = '%(table_name)s'
                        );"""
        var_dict = {'schema': schema, 'table_name':table_name}
        return self._query_to_df(query, var_dict)[0]

    def get_column_types(self, schema, table_name):
        """
            Get column names and data types from table in dict format.
        Parameters
        ----------
        schema : str
            The schema the table should be in.
        table_name : str
            The table name to search for.
        """
        query = """SELECT column_name, data_type 
                    FROM information_schema.columns
                    WHERE table_name = '%(table_name)s' 
                    AND table_schema = '%(schema)s';"""
        var_dict = {'schema': schema, 'table_name':table_name}
        return self._query_to_dict(query, var_dict)

    def append_to_table_return_ids(df, schema, table_name, id_column_name):
        colunms = '"' + '", "'.join(df.columns) + '"'
        values = ''
        for index, row in df.iterrows():
            values += '(' + 
        query = """WITH inserts as (
                        INSERT INTO %(schema)s.%(table_name)s
                        (columns)
                            VALUES (. . .)
                            RETURNING *
                    )
                    SELECT %(id_column_name)s
                    FROM inserts;"""

    def _query_to_df(self, query_str, var_dict):
        df = pd.read_sql_query(query_str, self.conn, params=var_dict)
        return df

    def _query_to_dict(self, query_str, var_dict):
        cur = self.conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
        cur.execute(query_str, var_dict)
        results = cur.fetchall()
        self.curser = conn.cursor()
        return results

    def _simple_execute(self, query_str, var_dict):
        self.curser.execute(query_str, var_dict)
        self.conn.commit()

    def __del__(self):
        self.conn.commit()
        self.conn.close()