from src import ConnectionBase
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
        """
        return True

    def get_column_types(self, schema, table_name):
        '''
            Hardcoded since would normally pull information from database.
        '''
        if table_name == 'src_credit':
            return {"cast": 'str', "crew": 'str', "id": 'Int64'}
        elif table_name == 'src_keywords':
            return {"id": 'Int64', "keywords": 'str'}
        elif table_name == 'src_links':
            return {"movieId": 'Int64', "imdbId": 'str',"tmdbId": 'str'}
        elif table_name == 'src_movies_metadata':
            return {"adult": 'str', "belongs_to_collection": 'str', "budget": 'str',
            "genre": 'str', "homepage": 'str', "id": 'Int64', "imdb_id": 'str',
            "original_language": 'str', "original_title": 'str', "overview": 'str',
            "popularity": 'str', "poster_path": 'str', "production_company": 'str',
            "production_countries": 'str', "release_date": 'str', "revenue": 'str',
            "runtime": 'str', "spoken_languages": 'str', "status": 'str', 
            "tagline": 'str', "title": 'str', "video": 'str', "vote_average": 'str',
            "vote_count": 'str'}
        elif table_name == 'src_ratings':
            return {"userId": 'Int64', "movieId": 'Int64', "rating": 'str'}
        else:
            raise RuntimeError(f'CSVConnection.get_column_types: Table name [{table_name}] not recognized.')

    def append_to_table_return_ids(self, df, schema, table_name, id_column_name):
        if table_name == 'file':
            # a special cases that is complicated with writing to csv since need a unique id back
            # with open instead of just pd.read_csv to ensure file exists
            file_name = f'{self.save_location}/{table_name}/{table_name}.csv'
            if not os.path.exists(file_name):
                pd.DataFrame(columns=df.columns+[id_column_name]).to_csv(file_name, index=False)
            df_old = pd.read_csv(file_name)
            # since can't rely on db to add id automatically, we have to create it and make sure it isn't already used
            if len(df_old.index) == 0:
                df[id_column_name] = list(range(1, len(df.index)+1))
            else:
                df[id_column_name] = list(range(int(df_old[id_column_name].iloc[-1]), (int(df_old[id_column_name].iloc[-1])+len(df.index)+1)))
            df.to_csv(file_name, index=False, mode='a', header=False)
            return df
        else:
            pass # will fill in if is needed

    def append_to_table(self, df, schema, table_name):
        file_name = f'{self.save_location}/{table_name}/{table_name}.csv'
        if not os.path.exists(file_name):
            df.to_csv(file_name, index=False)
        else:
            df.to_csv(file_name, index=False, mode='a', header=False)

    def get_data_mapping(self, mapping_schema, end_schema, end_table_name):
        df = pd.read_csv(f'{self.save_location}/data_mapping/data_mapping.csv')
        df = df.loc[(df['end_schema'] == end_schema) & (df['end_table'] == end_table_name)]
        return df

    def get_source_data(self, data_mapping):
        source_columns = data_mapping['source_field'].values
        end_columns = data_mapping['end_field'].values
        end_data_types = data_mapping['end_data_type'].values
        # json will need to be unpacked after loading initial dataframe
        table = data_mapping['source_table'].iloc[0]
        file_name = f'{self.save_location}/{table}/{table}.csv'
        # json will need to be unpacked into initial dataframe
        if 'json' in data_mapping['source_data_type'].values:
            data_dict = etl.read_csv_to_dict(file_name)
            to_unpack = (data_mapping.loc[data_mapping['source_data_type']=='json'])['source_field'].values
            not_to_unpack = etl.differance_between_lists(source_columns, to_unpack)
            to_unpack = list(set([x.split('.')[0] for x in to_unpack]))
            df = json_normalize(data=data_dict, record_path=to_unpack, meta=not_to_unpack)
        else:
            df = pd.read_csv(file_name, usecols=source_columns, dtype=pd.Series(end_data_types,index=source_columns).to_dict())
        return df.rename(columns=pd.Series(end_columns,index=source_columns).to_dict())

    def insert_with_conflict(self, df, data_mapping, schema, table_name):
        pass