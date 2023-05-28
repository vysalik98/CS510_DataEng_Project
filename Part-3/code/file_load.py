import os
import re
from bs4 import BeautifulSoup
import pandas as pd
from stop_trans import stop_val, stop_trans
from load_to_db import load_stop_data

'''
This script iterates through the stopevents data folder, extracts the data, 
conducts validations and transformations, and then loads the data into the database.
'''

# Function to process each HTML file
def process_html_file(file_path):
    with open(file_path, "r") as html_file:
        soup = BeautifulSoup(html_file, "lxml")
        trip = []
        header = soup.find_all("h2")
        print("Number of headers: " + str(len(header)))
        for h2s in header:
            num = re.search(r"\d+", h2s.text)
            if num:
                trip.append(num.group())
        print("Number of Trips: " + str(len(trip)))
        stopdata = []
        tables = soup.find_all('table')
        length = len(tables)
        print(length)
        for i in range(length):
            data = tables[i].find_all('td')
            dict = {}
            dict['trip_id'] = trip[i]
            dict['vehicle_id'] = data[0].string
            dict['route_id'] = data[3].string
            dict['direction'] = data[4].string
            dict['service_key'] = data[5].string
            stopdata.append(dict)
        print(len(stopdata))
        # print(stopdata)  # Print the stopdata list
        df = pd.DataFrame(stopdata)

        stop_val(df)
        stop_trans(df)
        load_stop_data(df)

# Folder containing HTML files
loc = "/home/vysali/stopdata"

# Iterate over each file in the folder
for filename in os.listdir(loc):
    if filename.endswith('.html'):
        file_path = os.path.join(loc, filename)
        print("Initiating file load :" +file_path)
        process_html_file(file_path)
