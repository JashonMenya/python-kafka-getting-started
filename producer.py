from argparse import ArgumentParser, FileType
from configparser import ConfigParser

from confluent_kafka import Producer

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['producer'])

    # Create a Kafka producer instance
    producer = Producer(config)

    # Specify the topic to produce to
    topic = "poems"

    # Produce messages in a loop
    for num in range(101):  # Loop from 0 to 100
        key = f"message_{num}"  # Key for the message
        value = f"Hello Kafka! This is message {num}"  # Value for the message

        # Produce the message to the topic with the specified key and value
        producer.produce(topic=topic, key=key.encode('utf-8'), value=value.encode('utf-8'))

        # Print a message for each produced message
        print(f"Produced message {num}")

        # Flush after producing a batch of messages
        if num % 10 == 0:
            producer.flush()

    # Wait for any outstanding messages to be delivered and delivery reports received
    producer.flush()

    print("Messages published successfully!")
