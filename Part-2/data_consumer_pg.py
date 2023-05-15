#!/usr/bin/env python

"""
Developer : Vysali Kallepalli
Script : data_consumer_pg.py
Date : 05/12/2023
Purpose : Consumer Data from Kafka & load postgres
Usage : ./data_consumer_pg.py config.ini trimet_data
"""

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import pandas as pd
import json
from data_transformations import data_validate1, data_transform, data_validate2
from postgres_loader import load_data_to_postgres


def parse_args():
    """
    Parse command line arguments
    """
    parser = ArgumentParser()
    parser.add_argument("config_file", type=FileType("r"))
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("topic_name", help="name of the Kafka topic")
    return parser.parse_args()


def parse_config(args):
    """
    Parse configuration file
    """
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser["default"])
    config.update(config_parser["consumer"])
    return config


def create_consumer(config):
    """
    Create and return Consumer instance
    """
    consumer = Consumer(config)
    return consumer


def reset_offset(consumer, partitions, reset):
    """
    Set message offset based on the reset flag
    """
    if reset:
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)


def consume_messages(consumer, batch_size):
    while True:
        data = []
        while True:
            messages = consumer.consume(num_messages=batch_size, timeout=1.0)
            if len(messages) == 0:
                print("Waiting for Messages...")
                continue

            for message in messages:
                if message.error():
                    print("Error while consuming message: {}".format(message.error()))
                else:
                    message_data = json.loads(message.value())
                    data.append(message_data)

            # check if batch is complete or partial
            if len(data) >= batch_size or (
                len(messages) < batch_size and len(data) > 0
            ):
                df = pd.DataFrame(data)
                print("Number of messages processing : " + str(df.shape[0]))
                data_validate1(df=df)
                transform_df = data_transform(df=df)
                transform_df["direction"] = "Unknown"
                transform_df["route_id"] = -1
                print("Number of messages applying transformations on : " + str(df.shape[0]))
                data_validate2(transform_df)
                load_data_to_postgres(transform_df)

            data.clear()

            if len(messages) < batch_size and len(data) > 0:
                df = pd.DataFrame(data)
                print("Number of messages processing : " + str(df.shape[0]))
                transform_df = data_transform(df=df)
                transform_df["tripdir_type"] = "Unknown"
                transform_df["route_id"] = -1
                print("Number of messages applying transformations on : " + str(df.shape[0]))
                load_data_to_postgres(transform_df)


def main():
    """
    Main function
    """
    args = parse_args()
    config = parse_config(args)
    consumer = create_consumer(config)
    topic = args.topic_name
    consumer.subscribe([topic])
    batch_size = 100000
    consume_messages(consumer, batch_size)


if __name__ == "__main__":
    main()
