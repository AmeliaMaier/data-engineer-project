from ConnectionHelper import getConnection
import ETLUtil as etl
import pandas as pd
from datetime import datetime

def main():
    """
        Takes the place of a control system calling micro services.
        This replacement will insure the correct order of events but not the highest efficency.
        A real control system would idealy auto-load files as they are placed in the correct folder(s).
        It would also call all microservices that can overlap at once to increase system speed.
    """
    csv_file_path = 'data_files/'
    connection = getConnection('CSV', dbname=csv_file_path)
    load_csv('movies_metadata.csv', 'schema', 'src_movies_metadata', csv_file_path, connection)
    load_csv('credits.csv', 'schema', 'src_credits', csv_file_path, connection)
    load_csv('keywords.csv', 'schema', 'src_keywords', csv_file_path, connection)
    load_csv('links.csv', 'schema', 'src_links', csv_file_path, connection)
    load_csv('ratings.csv', 'schema', 'src_ratings', csv_file_path, connection)
    # leaving these out under assumption they are subset of larger files.
    # load_csv('links_small.csv', 'schema', 'src_links', csv_file_path, connection)
    # load_csv('ratings_small.csv', 'schema', 'src_ratings', csv_file_path, connection)

def load_csv(csv_name, schema, table_name, file_path, connection):
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
        file_path : str
            The location of the csv file without the file name.
        connection: object
            The connection to use for writing data out and reading in from database/csv.
    """
    try:
        if connection.table_exists(schema, table_name):
            # read in csv without allowing autotyping
            data = etl.read_csv_types_assigned(f'{file_path}{csv_name}', connection.get_column_types(schema, table_name)) 
            # write out file metadata
            file_info = pd.DataFrame({'name': csv_name, 'ingested_date': datetime.now()})
            file_info = connection.append_to_table_return_ids(file_info, schema, table_name, 'id')
            # write out file data
            data['file_id'] = file_info['id'].iloc[-1]
            data['created_date'] = datetime.now() # this would be defaulted if writing directly to psql
            data['transformed'] = False # this would be defaulted if writing directly to psql
            connection.append_to_table(data, schema, table_name)
        else:
            raise RuntimeError(f'Main.load_csv: Table name [{table_name}] in schema [{schema}] not found. Injestions of file [{csv_name}] at [{file_path}] stopped.')
    except Exception as err:
        raise RuntimeError("Main.load_csv: {err}")


def load_table():
    pass
def load_report():
    pass

if __name__ == '__main__':
    main()