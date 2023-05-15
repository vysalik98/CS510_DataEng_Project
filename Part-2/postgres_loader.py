#!/usr/bin/env python

"""
Developer : Vysali Kallepalli
Script : postgres_loader.py
Date : 05/12/2023
Purpose : Function to Connect & load data to Postgres
"""

import psycopg2
import io


def load_data_to_postgres(df):
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="8424",
        host="localhost",
        port="5432",
        options=f"-c search_path=trimet",
    )
    cur = conn.cursor()
    print("Establishing connections to Postgress")
    # load distinct data to a temporary table
    temp_table_name = "temp_trip"
    cur.execute(f"CREATE TEMPORARY TABLE {temp_table_name} (LIKE trip);")
    cur.execute(
        f"ALTER TABLE {temp_table_name} ADD CONSTRAINT temp_trip_pkey PRIMARY KEY (trip_id, route_id, vehicle_id, service_key, direction);"
    )
    output = io.StringIO()
    df[
        ["trip_id", "route_id", "vehicle_id", "service_key", "direction"]
    ].drop_duplicates().to_csv(output, header=False, index=False)
    output.seek(0)
    cur.copy_from(output, temp_table_name, sep=",", null="")
    print("Loading data into trimet.trip")
    # insert distinct data into trip table
    cur.execute(
        f"INSERT INTO trip SELECT * FROM {temp_table_name} ON CONFLICT DO NOTHING;"
    )
    print("Loading data into trimet.breadcrumb")
    # load data to breadcrumb table
    output = io.StringIO()
    df[["tstamp", "latitude", "longitude", "speed", "trip_id"]].to_csv(
        output, header=False, index=False
    )
    output.seek(0)
    cur.copy_from(output, "breadcrumb", sep=",", null="")

    conn.commit()
    print("Loading data to prostgres is complete!")
    cur.close()
    conn.close()
