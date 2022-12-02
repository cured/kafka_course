from confluent_kafka import Consumer
import sys


class ConsumerDemo:
    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": "my-third-application",
            "auto.offset.reset": "earliest",
            "debug": "consumer"
        })


def main():
    topic = "demo_python"
    c = ConsumerDemo()
    c.consumer.subscribe([topic])
    try:
        while True:
            record = c.consumer.poll(1)
            if record is None:
                continue

            sys.stderr.write('%% Message delivered.\n'
                             'Topic: %s,\n'
                             'Key: %s,\n'
                             'Partition: [%d],\n'
                             'Offset: %d,\n'
                             'Timestamp: %s\n' %
                             (record.topic(), record.key(), record.partition(), record.offset(), record.timestamp()))
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        c.consumer.close()


if __name__ == "__main__":
    main()
