from ConnectionBase import ConnectionBase
import pandas as pd

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

    def append_to_table_return_ids(df, schema, table_name, id_column_name):
        pass


