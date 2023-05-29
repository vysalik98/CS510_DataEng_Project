#!/usr/bin/env python

"""
Purpose: Get Stop Events data from URL, convert to DataFrame, and produce to Kafka
Usage: ./stop_producer.py config.ini <topic_name>
"""

import argparse
import json
import urllib.request
from confluent_kafka import Producer
import configparser
from bs4 import BeautifulSoup
import pandas as pd
import re


def read_data_from_url(url):
    """
    Read Stop Events data from the URL
    Return decoded response
    """
    print("Reading data from URL")
    with urllib.request.urlopen(url) as response:
        data = response.read().decode("utf-8")
    return data


def convert_to_dataframe(html_data):
    """
    Convert HTML data to pandas DataFrame
    """
    print("Converting data to DataFrame")
    soup = BeautifulSoup(html_data, "html.parser")
    trip = get_id(soup)
    stopdata = []
    tables = soup.find_all("table")
    length = len(tables)
    for i in range(length):
        data = tables[i].find_all("td")
        dictionary = {}
        dictionary["trip_id"] = trip[i]
        dictionary["vehicle_id"] = data[0].string
        dictionary["route_id"] = data[3].string
        dictionary["direction"] = data[4].string
        dictionary["service_key"] = data[5].string
        stopdata.append(dictionary)
        service_keys = {data['service_key'] for data in stopdata}
        if service_keys:
            index = list(service_keys)[0]
            for data in stopdata:
                data["service_key"] = index
    df = pd.DataFrame(stopdata)
    return df


def get_id(soup):
    """
    Extract trip IDs from the HTML soup
    """
    trip = []
    headers = soup.find_all("h2")
    print("Number of headers: " + str(len(headers)))
    for header in headers:
        num = re.search(r"\d+", header.text)
        if num:
            trip.append(num.group())
    print("Number of Trips: " + str(len(trip)))
    return trip


def send_to_kafka(producer, topic_name, data, batch_size=10000):
    """
    Send data to specified Kafka topic
    """
    print("Sending data to Kafka")
    counter = 0
    for item in data:
        producer.produce(topic_name, json.dumps(item))
        counter += 1
        if counter % batch_size == 0:
            producer.flush()
            print(f"Pushed {counter} messages")
    producer.flush()
    print(f"Pushed {counter} messages to Kafka Topic in total")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send data to a Kafka topic")
    parser.add_argument("config_file", help="path to the config.ini file")
    parser.add_argument("topic_name", help="name of the Kafka topic")
    args = parser.parse_args()

    # Read the Kafka bootstrap servers from the config file
    config = configparser.ConfigParser()
    config.read(args.config_file)
    config = dict(config["default"])

    # Read data from the URL, convert to DataFrame & clean the data
    url = "http://www.psudataeng.com:8000/getStopEvents"
    html_data = read_data_from_url(url)
    df = convert_to_dataframe(html_data)

    # Send each row of DataFrame as a message to the specified Kafka topic
    producer = Producer(config)
    sent_all_data = send_to_kafka(producer, args.topic_name, df.to_dict(orient="records"))

    print("Process completed")