
class ConnectionBase:
    """
        This is being used as an information interface as Python doesn't support interfaces.
        No implementation should exist in this class.
        New classes with implementations for connection types should use the same method names a here
        so that they are interchangeable.
    """
    def __init__(self, conn):
        pass

    def table_exists(self, schema, table_name):
        """
        Checks if the table exists in the schema provided. Returns true if it exists and false if it does not.
        Parameters
        ----------
        schema : str
            The schema the table should be in.
        table_name : str
            The table name to search for.
        """
        pass

    def get_column_types(self, schema, table_name):
        """
        Get column names and data types from schema.table. Returns in dict format.
        Parameters
        ----------
        schema : str
            The schema the table should be in.
        table_name : str
            The table name to search for.
        """
        pass

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
        pass

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
        pass

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
        pass

    def get_source_data(self, data_mapping):
        """
        Pulls the data from the source data, casts it, and renames it. Returns as dataframe
        Parameters
        ----------
        data_mapping: pandas dataframe
            The dataframe of all the mapping information for the current transformations.
        """
        pass

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
        pass