#!/usr/bin/env python

"""
Developer : Vysali
Script : gather_data.py
Date : 04/22/2023
Purpose : Get Trimet data through API Call & Produce to Kafka
Usage : ./data_producer.py config.ini trimet_data
"""

import argparse
import json
import urllib.request
from confluent_kafka import Producer
import configparser


def read_data_from_url(url):
    """
    Read Trimet data from the URL
    return decoded response
    """
    print("Reading data from URL")
    with urllib.request.urlopen(url) as response:
        data = response.read().decode("utf-8")
    return data


# def load_data_to_file(data, filename):
#     """
#     Load the data into a specified JSON file
#     """
#     print("Loading data into a file")
#     with open(filename, "w") as f:
#         json.dump(data, f)
#     print(f"Data saved to {filename}")


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

    # Read data from the URL & clean the data
    url = "http://psudataeng.com:8000/getBreadCrumbData"
    data = json.loads(read_data_from_url(url))

    # Send each sensor reading to the specified Kafka topic
    producer = Producer(config)
    sent_all_data = False
    while not sent_all_data:
        sent_all_data = send_to_kafka(producer, args.topic_name, data)

    print("Process completed")
