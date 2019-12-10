
class ConnectionBase:
    """
        This is being used as an information interface as Python doesn't support interfaces.
        No implementation should exist in this class.
        New classes with implementations for connection types should use the same method names a here
        so that they are interchangeable.
    """
    def table_exists(self, schema, table_name):
        pass

    def get_column_types(self, schema, table_name):
        pass

    def append_to_table_return_ids(self, df, schema, table_name, id_column_name):
        pass

    def append_to_table(self, df, schema, table_name):
        pass

    def get_data_mapping(self, mapping_schema, end_schema, end_table_name):
        pass

    def get_source_data(self, data_mapping):
        pass