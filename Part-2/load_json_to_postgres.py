#!/usr/bin/env python

"""
Developer : Vysali Kallepalli
Script : load_json_to_postgres.py
Date : 05/12/2023
Purpose : Load data from files to Postgres Tables
Usage : python3 load_json_to_postgres.py
"""

import glob
import os
import pandas as pd
from data_transformations import data_validate1, data_transform, data_validate2
from postgres_loader import load_data_to_postgres


loc = "/home/vysali/data/"

for file_path in glob.glob(loc + "/*"):
    if not os.path.isdir(file_path):
        with open(file_path, "r") as file:
            data = file.read()
            if '{"data not available for this day"}' in data:
                print("No data to process in the file:" + file_path)
                continue
        file_full_path = loc + os.path.basename(file_path)
        print("Initiating data load from the file : " + file_full_path)
        df = pd.read_json(file_full_path, orient='records', lines=True)
        data_validate1(df=df)
        transformed_df = data_transform(df=df)
        transformed_df["route_id"] = -1
        transformed_df["direction"] = "UNKNOWN"
        data_validate2(transformed_df)
        load_data_to_postgres(df=transformed_df)
