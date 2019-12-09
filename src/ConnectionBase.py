
class ConnectionBase:
    """
        This is being used as an information interface as Python doesn't support interfaces.
        No implementation should exist in this class.
        New classes with implementations for connection types should use the same method names a here
        so that they are interchangeable.
    """
    def table_exists(self, schema, table_name):
        pass