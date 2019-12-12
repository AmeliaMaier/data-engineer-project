from src.ConnectionHelper import getConnection
from src import ETLUtil as etl
import pandas as pd
from datetime import datetime


def main():
    """
        Takes the place of a control system calling micro services.
        This replacement will insure the correct order of events but not the highest efficency.
        A real control system would idealy auto-load files as they are placed in the correct folder(s).
        It would also call all microservices that can overlap at once to increase system speed.
    """
    csv_file_path = 'data_files'
    result_file_path = 'data_files/results'
    source_backup_path = 'data_files/source_backup'

    connection = getConnection('CSV', dbname=csv_file_path)
    # load_csvs can happen in any order
    # load_csv('movies_metadata.csv', result_file_path, 'src_movies_metadata', csv_file_path, source_backup_path, connection)
    # load_csv('credits.csv', result_file_path, 'src_credits', csv_file_path, source_backup_path, connection)
    # load_csv('keywords.csv', result_file_path, 'src_keywords', csv_file_path, source_backup_path, connection)
    # load_csv('links.csv', result_file_path, 'src_links', csv_file_path, source_backup_path, connection)
    # load_csv('ratings.csv', result_file_path, 'src_ratings', csv_file_path, source_backup_path, connection)
    # # leaving these out under assumption they are subset of larger files.
    # # load_csv('links_small.csv', result_file_path, 'src_links', csv_file_path, source_backup_path, connection)
    # # load_csv('ratings_small.csv', result_file_path, 'src_ratings', csv_file_path, source_backup_path, connection)

    # step 1 in load_tables
    # load_table(result_file_path,'movies', connection)
    # load_table(result_file_path,'collections', connection)
    # load_table(result_file_path,'genres', connection)
    # load_table(result_file_path,'production_companies', connection)
    ## these two not needed because international standards should be filled in already
    ## load_table('production_countries')
    ## load_table('spoken_languages')
    # load_table(result_file_path,'keywords', connection)
    # load_table(result_file_path,'cast_members', connection)
    # load_table(result_file_path,'crew_members', connection)
    # step 2 in load_tables
    # load_table(result_file_path,'movie_collections', connection)
    # load_table(result_file_path,'movie_genres', connection)
    # load_table(result_file_path,'movie_production_companies', connection)
    load_table(result_file_path,'movie_production_countries', connection)
    load_table(result_file_path,'movie_spoken_languages', connection)
    load_table(result_file_path,'movie_keywords', connection)
    load_table(result_file_path,'movie_ratings', connection)
    load_table(result_file_path,'movie_cast_members', connection)
    load_table(result_file_path,'movie_crew_members', connection)
    load_table(result_file_path,'movie_links', connection)


def load_csv(csv_name, schema, table_name, src_file_path, source_backup_path, connection):
    """
        This method reads in the raw source data, saves the file metadata, 
        and saves the source data with as little change as possible.
        No data cleaning or transformation is done at this stage because 
        leaving it raw allows for the full data warehouse to be recreated 
        if cleaning/transformation/ect rules change in the future.
        It also allows for end-to-end auditability if an issue is found at
        in the future or questions are asked.
        Parameters
        ----------
        csv_name : str
            The name, excluding file path, for the csv to be loaded into a src table.
        schema : str
            The schema the table should be in.
        table_name : str
            The name of the table to write the data to.
        src_file_path : str
            The location of the csv file without the file name.
        source_backup_path : str
            The location the csv will be moved to after being used.
        connection: object
            The connection to use for writing data out and reading in from database/csv.
    """
    try:
        if connection.table_exists(schema, table_name):
            source_filename = f'{src_file_path}/{csv_name}'
            backup_filename = f'{source_backup_path}/{csv_name.split(".")[0]}_{etl.get_random_id()}.csv'
            # read in csv without allowing autotyping
            data = etl.read_csv_types_assigned(source_filename, connection.get_column_types(schema, table_name))
            # write out file metadata
            file_info = pd.DataFrame({'name': [csv_name], 'ingested_date': [datetime.now()], 'backup_name': backup_filename})
            file_info = connection.append_to_table_return_ids(file_info, schema, 'file', 'id')
            # write out file data
            data['file_id'] = file_info['id'].iloc[-1]
            # this section of column additions would be defaulted in the database in a real system
            data['created_date'] = datetime.now() 
            data = _get_transformed_columns(data, table_name) 
            connection.append_to_table(data, schema, table_name)
            # move source file to save location, renamed to endure unique
            etl.move_file(source_filename, backup_filename)
        else:
            raise RuntimeError(f'Main.load_csv: Table name [{table_name}] in schema [{schema}] not found. Injestions of file [{csv_name}] at [{src_file_path}] stopped.')
    except Exception as err:
        raise RuntimeError(f"Main.load_csv: {err}")


def load_table(schema, table_name, connection):
    """
        This could be much cleaner and simpler if it was not csv compatible but 
        relied only on the database instead. 

        This method pulls the data-mapping that applies to the schema.table_name,
        uses the mapping to pull the data from the source table, renames columns
        and casts as needed, and saved the data to the schema.table_name. 
        Parameters
        ----------
        schema : str
            The schema the table should be in.
        table_name : str
            The name of the table to write the data to.
        connection: object
            The connection to use for writing data out and reading in from database/csv.
    """
    if not connection.table_exists(schema, table_name):
        raise RuntimeError(f'Main.load_table: Table [{table_name}] needed to write out not found.')
    # load data mapping for given table
    data_mapping = connection.get_data_mapping(schema, schema, table_name)
    # load source data for new table
    source_data = connection.get_source_data(data_mapping)
    # basic data cleaning (casting to correct type) being done durring read in
    # more advanced cleaning would require business logic and would go here 
    # write out data
    on_conflict = data_mapping['table_type'].values[-1]
    df = connection.append_to_table(source_data, schema, table_name, on_conflict=on_conflict)
    # if type delete, check for returned conflicts
    if df is not None:
        ids_to_append = df[f'{table_name}_id'].values
        to_append_df = source_data.loc[source_data['id'] in ids_to_append]
        connection.append_to_table(source_data, schema, table_name)
    # set source data as transformed
    connection.update_source_table(source_data, data_mapping)

def load_report():
    """
        Has not been filled in as not part of the current assignment.
    """
    pass

def _get_transformed_columns(df, table_name):
    if table_name == 'src_credits':
        df['transformed_cast_members'] = False
        df['transformed_crew_members'] = False
        df['transformed_movie_cast_members'] = False
        df['transformed_movie_crew_members'] = False
    elif table_name == 'src_keywords':
        df['transformed_keywords'] = False
        df['transformed_movie_keywords'] = False
    elif table_name == 'src_links':
        df['transformed_movie_links'] = False
    elif table_name == 'src_ratings':
        df['transformed_movie_ratings'] = False
    elif table_name == 'src_movie_metadata':
        df['transformed_movies'] = False
        df['transformed_collections'] = False
        df['transformed_genres'] = False
        df['transformed_production_companies'] = False
        df['transformed_movie_collections'] = False
        df['transformed_movie_genres'] = False
        df['transformed_movie_production_companies'] = False
        df['transformed_movie_production_contries'] = False
        df['transformed_movie_spoken_languages'] = False
    return df


if __name__ == '__main__':
    main()