import pandas as pd

def read_csv_types_assigned(file_name, dtype):
    return pd.read_csv(file_name, dtype=dtype)
