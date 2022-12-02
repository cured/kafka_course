from confluent_kafka import Producer
import sys


def callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered.\n'
                         'Topic: %s,\n'
                         'Key: %s,\n'
                         'Partition: [%d],\n'
                         'Offset: %d,\n'
                         'Timestamp: %s\n' %
                         (msg.topic(), msg.key(), msg.partition(), msg.offset(), msg.timestamp()))


class ProducerDemo:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": "127.0.0.1:9092",
        })


def main():
    p = ProducerDemo()
    for i in range(10):
        topic = "demo_python"
        value = f"hello world {i}"
        key = f"id_{i}"
        p.producer.produce(topic=topic, key=key, value=value, callback=callback)
    p.producer.flush()


if __name__ == "__main__":
    main()
