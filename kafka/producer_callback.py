from confluent_kafka import Producer
import sys


def callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d on %s\n' %
                         (msg.topic(), msg.partition(), msg.offset(), msg.timestamp()))


class ProducerDemo:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": "127.0.0.1:9092",
        })


def main():
    p = ProducerDemo()
    for i in range(10):
        p.producer.produce(topic="demo_python", value=f"love sw {i}", callback=callback)
    p.producer.flush()


if __name__ == "__main__":
    main()
