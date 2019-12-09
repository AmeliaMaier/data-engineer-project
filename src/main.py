from ConnectionHelper import getConnection

def main():
    """
        Takes the place of a control system calling micro services.
        This replacement will insure the correct order of events but not the highest efficency.
        A real control system would idealy auto-load files as they are placed in the correct folder(s).
        It would also call all microservices that can overlap at once to increase system speed.
    """
    connection = getConnection('CSV', dbname=None)
    csv_file_path = 'data_files/'
    load_csv('movies_metadata.csv', 'movies_metadata', csv_file_path, connection)
    load_csv('credits.csv', 'credits', csv_file_path, connection)
    load_csv('keywords.csv', 'keywords', csv_file_path, connection)
    load_csv('links.csv', 'links', csv_file_path, connection)
    load_csv('ratings.csv', 'ratings', csv_file_path, connection)
    # this is an example of how a different file name can still be loaded to the correct table
    load_csv('links_small.csv', 'links', csv_file_path, connection)
    load_csv('ratings_small.csv', 'ratings', csv_file_path, connection)

def load_csv(csv_name, csv_type, file_path, connection):
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
        csv_type : str
            The sound the animal makes
        file_path : str
            The location of the csv file without the file name.
    """
    pass
def load_table():
    pass
def load_report():
    pass

if __name__ == '__main__':
    main()