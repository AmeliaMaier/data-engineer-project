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

    def append_to_table_return_ids(self, df, schema, table_name, id_column_name):
        """
        Pandas has the ability to append directly to a table but it doesn't allow 
        for upsert or for returning the ids yet, so custom sql is currently a
        better option.
        """
        insert_statement, var_dict = self._get_insert_statment_from_df(df, schema, table_name)
        query = f"""WITH inserts as (
                        {insert_statement}
                        RETURNING *
                    )
                    SELECT %(id_column_name)s, 
                    "{'", "'.join(df.columns) }"
                    FROM inserts;"""
        var_dict['id_column_name'] = id_column_name
        return self._query_to_df(query, var_dict)

    def append_to_table(self, df, schema, table_name):
        """
        Simple append statement, not taking upsert or confilicts into account yet.
        """
        insert_statement, var_dict = self._get_insert_statment_from_df(df, schema, table_name)
        insert_statement += ';'
        self._simple_execute(insert_statement, var_dict)

    def _get_insert_statment_from_df(self, df, schema, table_name):
        columns = self._get_column_statment_from_df(df)
        value_statment, var_dict = self._get_value_statement_from_df(df)
        var_dict['schema'] = schema
        var_dict['table_name'] = table_name
        insert_statement = f"""INSERT INTO %(schema)s.%(table_name)s
                                {columns} VALUES {value_statment}"""
        return insert_statement, var_dict

    def _get_column_statment_from_df(self, df):
        """
        Builds up the columns section of the statment like:
            (column1, column2, …)
        with quotes around each to ensure they aren't interpreted as control
        words
        """
        return '("' + '", "'.join(df.columns) + '")'

    def _get_value_statement_from_df(self, df):
        """
        This builds up the values section of the statment like:
            (value1, value2, …),
            (value1, value2, …) ,..
        with the corresponding variable dictionary to avoid sql
        injection.    
        """
        values_statement = []
        var_dict = dict({})
        for index, row in df.iterrows():
            statement = []
            for i, value in row.items():
                statement.append(f'%(var{index}.{i})s')
                var_dict[f'var{index}.{i}'] = value
            values_statement.append(f'({", ".join(statement)})')
        values = ', '.join(values_statement)
        return values, var_dict

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