#!/usr/bin/env python

"""
Purpose : Consume StopEvents data from Kafka, apply transformations, and push to a PostgreSQL table
Usage : ./stop_consumer.py config.ini <topic_name>
"""
import pandas as pd
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from stop_trans import stop_val, stop_trans
from load_to_db import load_stop_data



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

def consume_messages(consumer, reset):
    """
    Consume messages from Kafka, apply transformations, and write them to PostgreSQL Database
    """
    try:
        while True:
            msg = consumer.poll(2.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                key = msg.key()
                value = msg.value()
                data_str = value.decode('utf-8')
                data_dict = eval(data_str)
                df = pd.DataFrame(data_dict, index=[0])
                stop_val(df)
                stop_trans(df)
                load_stop_data(df)
                reset_offset(consumer, msg.partition(), reset)
            
    
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


def main():
    """
    Main function
    """
    args = parse_args()
    config = parse_config(args)
    consumer = create_consumer(config)
    topic = args.topic_name
    consumer.subscribe([topic])
    print("Consuming messages from topic: {}".format(topic))
    consume_messages(consumer, args.reset)
    print("Validated and Loaded data to Postgres Successfully")


if __name__ == "__main__":
    main()