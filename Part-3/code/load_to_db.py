import psycopg2
from psycopg2.extras import execute_values

'''
Purpose : This script imports data into the "stopevents" table in a PostgreSQL database
'''

def load_stop_data(df):
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="8424",
        host="localhost",
        port="5432",
        options=f"-c search_path=trimet",
    )
    cur = conn.cursor()
    print("Establishing connections to PostgreSQL")
    print("Loading data into trimet.stopevents")

    # Converting the filtered DataFrame to a list of tuples
    records = df[["trip_id", "route_id", "vehicle_id", "service_key", "direction"]].to_records(index=False)
    data = list(records)

    # INSERT statement
    insert_query = """
        INSERT INTO trimet.stopevents (trip_id, route_id, vehicle_id, service_key, direction)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    execute_values(cur, insert_query, data)

    conn.commit()
    print("Loading data to PostgreSQL is complete!")
    cur.close()
    conn.close()