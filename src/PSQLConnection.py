from src.ConnectionBase import  ConnectionBase
import psycopg2
import psycopg2.extras
import pandas as pd


class PSQLConnection(ConnectionBase):
    """
    ******* BETA - NOT TESTED ***********************************
        An implementation of connection for psql as you would never 
        actually transform source csvs just to save them as csvs and 
        then import them into a database manually.
        This section has not been tested and is inteded as a framework 
        for what is needed. There are likely more efficient ways for 
        it to work if it does not also need to be compatible with a csv 
        driven design.
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
        Pandas has the ability to append directly to a database table but it doesn't allow 
        for upsert or for returning the ids yet, so custom sql is currently a
        better option.
        Appends the data to the schema.table_name and returns the values created in the 
        id_column_name.
        Parameters
        ----------
        df: pandas dataframe
            The data to be appended to the table.
        schema : str
            The schema the table should be in.
        table_name : str
            The table name to search for.
        id_column_name: str
            The name of the column to be returned.
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

    def append_to_table(self, df, schema, table_name, on_conflict='append'):
        """
        Appends the data to the schema.table_name and responds if there is a conflict.
        Parameters
        ----------
        df: pandas dataframe
            The data to be appended to the table.
        schema : str
            The schema the table should be in.
        table_name : str
            The table name to search for.
        on_conflict: str
            'append' or 'delete'. If 'append' the it does nothing on a conflict. 
            If 'delete' then it marks the column 'deleted' as True, sets 'deleted_date' as now(),
            and returns the {table_name}_id for that row.
        """
        insert_statement, var_dict = self._get_insert_statment_from_df(df, schema, table_name)
        if on_conflict == 'delete':
            insert_statement += f'ON CONFLICT(unique_{table_name}) DO UPDATE SET deleted=TRUE, deleted_date=now() RETURNING {table_name}_id;'
            return self._query_to_df(insert_statement, var_dict)
        else:
            insert_statement += f' ON CONFLICT (unique_{table_name}) DO NOTHING;'
            self._simple_execute(insert_statement, var_dict)

    def get_data_mapping(self, mapping_schema, end_schema, end_table_name):
        """
        Pulls the data-mapping for the table to be filled. Returns data as a dataframe.
        Parameters
        ----------
        mapping_schema: str
            The schema the data_mapping table is in.
        end_schema : str
            The schema the table to be filled is in.
        end_table_name : str
            The table name to be filled.
        """
        query = '''SELECT * FROM %(mapping_schema)s.data_mapping
                    WHERE "end_schema" = %(end_schema)s
                    AND "end_table" = %(end_table_name)s;'''
        var_dict = {'mapping_schema': mapping_schema, 'end_schema': end_schema, 'end_table_name': end_table_name}
        return self._query_to_df(query, var_dict)

    def get_source_data(self, data_mapping):
        """
        This assumes that only one source feeds to each table, as per design to 
        all for full tracking of data from start to end.
        **This design is not ideal. It needs to be refactored so that this class doesn't know 
        how data_mapping is formatted but is simply passed the information it needs.
        Pulls the data from the source data, casts it, and renames it. Returns as dataframe
        Parameters
        ----------
        data_mapping: pandas dataframe
            The dataframe of all the mapping information for the current transformations.
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

    def update_source_table(self, source_data, data_mapping):
        """
        **This design is not ideal. It needs to be refactored so that this class doesn't know 
        how data_mapping is formatted but is simply passed the information it needs.
        Updates the source table to show that the transformation to the end table has been done
        for the given dataset.
        Parameters
        ----------
        source_data: pandas dataframe
            The dataframe that contains all the data the was transformed and saved in the 
            data warehouse.
        data_mapping: pandas dataframe
            The dataframe of all the mapping information for the current transformations.
        """
        ids_to_update = set(source_data['id'].values)
        file_ids_to_update = set(source_data['file_id'].values)
        column_to_update = f'transformed_{source_data["end_table"].iloc[0]}'
        table_to_update = source_data["source_table"].iloc[0]
        schema_to_update = source_data["source_schema"].iloc[0]
        '%(end_schema)s'
        query = """UPDATE %(schema_to_update)s.%(table_to_update)s
                        SET \"%(column_to_update)s\" = TRUE
                        WHERE
                           \"id\" in %(ids_to_update)s
                           AND \"file_id\" in %(file_ids_to_update)s;"""
        var_dict = {'ids_to_update': tuple(ids_to_update), 'file_ids_to_update': tuple(file_ids_to_update),
                    'column_to_update': column_to_update, 'table_to_update': table_to_update,
                    'schema_to_update': schema_to_update}
        self._simple_execute(query, var_dict)

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