from ConnectionBase import ConnectionBase
import pandas as pd

class CSVUtil(ConnectionBase):
    def __init__(self, save_location):
        self.save_location = save_location

    def table_exists(self, schema, table_name):
        """
            Included so the the different connection types are interchangable. 
            Table or file existance of this type doesn't matter for csv work.
        """
        return True