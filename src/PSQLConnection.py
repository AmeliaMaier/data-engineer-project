from src import ConnectionBase
import psycopg2
import psycopg2.extras
import pandas as pd


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

    def append_to_table(self, df, schema, table_name, on_conflict='nothing'):
        """
        Simple append statement, not taking upsert or confilicts into account yet.
        """
        insert_statement, var_dict = self._get_insert_statment_from_df(df, schema, table_name)
        if on_conflict == 'delete':
            insert_statement += 'ON CONFLICT("user", "contact") DO UPDATE SET deleted=TRUE, deleted_date=now() RETURNING id;'
            return self._query_to_df(insert_statement, var_dict)
        else:
            insert_statement += ' ON CONFLICT ("id") DO NOTHING;'
            self._simple_execute(insert_statement, var_dict)

    def get_data_mapping(self, mapping_schema, end_schema, end_table_name):
        query = '''SELECT * FROM %(mapping_schema)s.data_mapping
                    WHERE "end_schema" = %(end_schema)s
                    AND "end_table" = %(end_table_name)s;'''
        var_dict = {'mapping_schema': mapping_schema, 'end_schema': end_schema, 'end_table_name': end_table_name}
        return self._query_to_df(query, var_dict)

    def get_source_data(self, data_mapping):
        """
        This assumes that only one source feeds to each table, as per design to 
        all for full tracking of data from start to end.
        """
        query = 'SELCT '
        columns = []
        for index, row in data_mapping.iterrows():
            if row['source_data_type'] == 'json':
                column = f"CAST(SPLIT_PART({row['source_field']}, '.', 1) ->>'SPLIT_PART({row['source_field']}, '.', 2)' as {row['end_data_type']}) as \"{row['end_field']}\""
            else:
                column = f'CAST("{row["source_field"]}" as {row["end_data_type"]}) as "{row["end_field"]}"'
            columns.append(column)
        query += f"""{', '.join(columns)} 
                    FROM {data_mapping['source_schema'].iloc[0]}.{data_mapping['source_table'].iloc[0]} 
                    WHERE \"transformed_{data_mapping['end_table'].iloc[0]}\" IS FALSE;"""
        return self._query_to_df(query)


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

    def _query_to_df(self, query_str, var_dict=None):
        df = pd.read_sql_query(query_str, self.conn, params=var_dict)
        return df

    def _query_to_dict(self, query_str, var_dict=None):
        cur = self.conn.cursor(cursor_factory = psycopg2.extras.DictCursor)
        cur.execute(query_str, var_dict)
        results = cur.fetchall()
        self.curser = self.conn.cursor()
        return results

    def _simple_execute(self, query_str, var_dict=None):
        self.curser.execute(query_str, var_dict)
        self.conn.commit()

    def __del__(self):
        self.conn.commit()
        self.conn.close()