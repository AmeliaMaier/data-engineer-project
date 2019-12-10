import pandas as pd
import csv

def read_csv_types_assigned(file_name, dtype):
    return pd.read_csv(file_name, dtype=dtype)

def read_csv_to_dict(file_name):
    reader = csv.reader(open(file_name, 'r'))
    d = dict({})
    for row in reader:
        k, v = row
        d[k] = v
    return d

def differance_between_lists(li1, li2): 
    return (list(set(li1) - set(li2))) 