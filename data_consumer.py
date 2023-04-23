#!/usr/bin/env python

"""
Developer : Vysali
Script : consumer.py
Date : 04/22/2023
Purpose : Consume Trimet data from Kafka and write to a File
Usage : ./consumer.py config.ini 
"""

import os
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import datetime


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


def get_file_name(topic):
    """
    Generate file name based on topic and file counter
    """
    return f"{topic}_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"


def write_message_to_file(f, topic, key, value):
    """
    Write message to file
    """
    if key is None:
        key = ""
    else:
        key = key.decode("utf-8")
    if value is not None:
        value = value.decode("utf-8")
    f.write(f"{value}\n")


def consume_messages(consumer, topic, reset):
    """
    Consume messages from Kafka and write them to a file
    """
    file_name = get_file_name(topic)
    f = open(file_name, "w")
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
                write_message_to_file(f, topic, key, value)
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
    consume_messages(consumer, topic, args.reset)


if __name__ == "__main__":
    main()
