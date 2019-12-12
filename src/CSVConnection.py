from src.ConnectionBase import ConnectionBase
import pandas as pd
from pandas.io.json import json_normalize
import os
from src import ETLUtil as etl


class CSVConnection(ConnectionBase):
    def __init__(self, save_location):
        self.save_location = save_location

    def table_exists(self, schema, table_name):
        """
            Included so the the different connection types are interchangable. 
            Table or file existance of this type doesn't matter for csv work.
        Checks if the table exists in the schema provided. Returns true if it exists and false if it does not.
        Parameters
        ----------
        schema : str
            The schema the table should be in.
        table_name : str
            The table name to search for.
        """
        return True

    def get_column_types(self, schema, table_name):
        '''
            Hardcoded since would normally pull information from database and Pandas 
            does a decent job of auto-casting.
        Get column names and data types from schema.table. Returns in dict format.
        Parameters
        ----------
        schema : str
            The schema the table should be in.
        table_name : str
            The table name to search for.
        '''
        return None

    def append_to_table_return_ids(self, df, schema, table_name, id_column_name):
        """
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
        if table_name == 'file':
            # a special cases that is complicated with writing to csv since need a unique id back
            # with open instead of just pd.read_csv to ensure file exists
            file_name = f'{schema}/{table_name}.csv'
            if not os.path.exists(file_name):
                open(file_name, 'a').close()
                pd.DataFrame(columns=list(df.columns)+[id_column_name]).to_csv(file_name, index=False)
            df_old = pd.read_csv(file_name)
            # since can't rely on db to add id automatically, we have to create it and make sure it isn't already used
            if len(df_old.index) == 0:
                df[id_column_name] = list(range(1, len(df.index)+1))
            else:
                df[id_column_name] = list(range(int(df_old[id_column_name].iloc[-1])+1, (int(df_old[id_column_name].iloc[-1])+len(df.index)+1)))
            df.to_csv(file_name, index=False, mode='a', header=False)
            return df
        else:
            pass # will fill in if is needed

    def append_to_table(self, df, schema, table_name, on_conflict='append'):
        '''
        Since this class is only intended to be used for the initial historical data, it ignores the on conflict data.
        See assumptions.
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
        '''
        file_name = f'{schema}/{table_name}.csv'
        if not os.path.exists(file_name):
            df.to_csv(file_name, index=False)
        else:
            df.to_csv(file_name, index=False, mode='a', header=False)

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
        df = pd.read_csv(f'{mapping_schema}/data_mapping.csv')
        df = df.loc[(df['end_schema'] == end_schema) & (df['end_table'] == end_table_name)]
        return df

    def get_source_data(self, data_mapping):
        """
        **This is a very inefficient method because of the need to unpack the not quite json in movie_metadata.
        **In a real project, I would contact the data vendor and have them fix the formatting problems 
        **in the dataset rather than trying to work around it.
        **Because the names are surounded by single quotes and there are single quotes in the data that 
        aren't properly escaped, it is impossible to ingest all the data correctly without manually going
        in and cleaning the csv files. The current solution is an 80-20 tradeoff.

        Pulls the data from the source data, casts it, and renames it. Returns as dataframe
        Parameters
        ----------
        data_mapping: pandas dataframe
            The dataframe of all the mapping information for the current transformations.
        """
        source_columns = data_mapping['source_field'].values
        end_columns = data_mapping['end_field'].values
        end_data_types = data_mapping['end_data_type'].values
        # json will need to be unpacked after loading initial dataframe
        table = data_mapping['source_table'].iloc[0]
        schema = data_mapping['source_schema'].iloc[0]
        file_name = f'{schema}/{table}.csv'
        # json will need to be unpacked into initial dataframe
        if 'json' in data_mapping['source_data_type'].values:
            to_unpack = (data_mapping.loc[data_mapping['source_data_type']=='json'])['source_field'].values
            not_to_unpack = etl.differance_between_lists(source_columns, to_unpack)
            unpack_limited = list(set([x.split('.')[0] for x in to_unpack]))
            source_data = pd.read_csv(file_name, usecols=not_to_unpack+unpack_limited)
            for unpack in unpack_limited:
                source_data = etl.flatten_dataframe_column(source_data, unpack, not_to_unpack)
        else:
            source_data = pd.read_csv(file_name, usecols=source_columns)
        source_data.rename(columns=dict(zip(source_columns, end_columns)), inplace=True)
        source_data.drop_duplicates(inplace=True)
        return source_data[end_columns]

    def update_source_table(self, source_data, data_mapping):
        """
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
        table_transformed = data_mapping["end_table"].values[-1]
        table_to_update = data_mapping["source_table"].values[-1]
        schema_to_update = data_mapping["source_schema"].values[-1]
        old_source = pd.read_csv(f'{schema_to_update}/{table_to_update}.csv')
        old_source[f'transformed_{table_transformed}'] = True
        old_source.to_csv(f'{schema_to_update}/{table_to_update}.csv', index=False)