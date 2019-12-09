import psycopg2
from CSVConnection import CSVConnection
from PSQLConnection import PSQLConnection

def getConnection(type, user_name=None, password=None, dbname=None, host=None):
    """
        Idealy user_name, password, dbname, and host would be 
        environment variables or saved in a secret store that 
        the program has access to rather than being passed as 
        parameters.
        Parameters
        ----------
        type : str
            The type of connection to be used for reading and writing data from the database (or csv files).
        user_name : str
            The user name for the database.
        password : str
            The password for the database.
        dbname : str
            The database name.
        host : str
            The host for the database connection.
    """
    try:
        if type == 'PSQL':
            conn = psycopg2.connect(dbname=dbname, user=user_name, password=password, host=host)
            return PSQLConnection(conn) 
        elif type == 'CSV':
            # using dbname as equivalent to the location files should be saved as the files represent the db.
            return CSVConnection(save_location=dbname)
        else:
            raise RuntimeError(f'ConnectionHelper.getConnection: Connection type [{type}] not recognized.')
    except Exception as err:
        raise RuntimeError(f'ConnectionHelper.getConnection: {err}')
