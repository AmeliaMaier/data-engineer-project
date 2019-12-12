import pandas as pd
import csv
import shutil
import uuid
import json

def read_csv_types_assigned(file_name, dtype):
    """
    Reads in a csv and returns a pandas dataframe.
    Parameters
    ----------
    file_name : str
        The file name with full or relative path.
    dtype : dictionary
        The column names and their types. None if you want 
        auto typing
    """
    return pd.read_csv(file_name, dtype=dtype)

def read_csv_to_list_of_dict(file_name):
    d = csv.DictReader(open(file_name), delimiter=',')
    list_of_dicts = [x for x in d]
    return list_of_dicts

def differance_between_lists(li1, li2): 
    return (list(set(li1) - set(li2)))

def move_file(original_file, new_file):
    shutil.move(original_file, new_file)

def get_random_id():
    return uuid.uuid1()

def flatten_dataframe_column(df, column, index_columns):
    df = df.dropna(axis='index', how='all', subset=[column])
    new_df = pd.DataFrame()
    for index, row in df.iterrows():
        try:
            index_column_values = row[index_columns].to_dict()
            json_acceptable_string = str(row[column]).strip("'<>() ").replace(": None", ": null").replace("'", "\"")
            if '[' in json_acceptable_string and ']' in json_acceptable_string:
                temp_df = pd.DataFrame(json.loads(json_acceptable_string, strict=False))
            else:
                temp_df = pd.DataFrame([json.loads(json_acceptable_string, strict=False)])
            temp_df.columns = [f'{column}.{x}' for x in temp_df.columns]
            for key, value in index_column_values.items():
                temp_df[key] = value
            new_df = pd.concat([new_df, temp_df])
        except Exception as err:
            print(f'ETLUtil.flatten_dataframe_column: Error loading row [{str(row)}]. {err}')
    return new_df