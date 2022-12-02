from confluent_kafka import Producer
import json
import sys
import time

from sseclient import SSEClient as EventSource


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


class WikimediaProducer:

    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": "127.0.0.1:9092",
            "enable.idempotence": "true",
            "linger.ms": 20,
            "batch.size": 32*1024,
            "compression.type": "snappy"
        })


class WikimediaChangeHandler:
    topic = "wikimedia.recentchange"
    producer: WikimediaProducer

    def __init__(self, p: WikimediaProducer):
        self.producer = p

    def send(self, *, message: str):
        sys.stderr.write(f"Message: {message}")
        self.producer.producer.produce(self.topic, str.encode(message))


def main():
    url = "https://stream.wikimedia.org/v2/stream/recentchange"

    p = WikimediaProducer()

    handler = WikimediaChangeHandler(p=p)
    for event in EventSource(url, last_id=None):
        if event.event == 'message':
            try:
                handler.send(message=event.data)
            except ValueError:
                sys.stderr.write("Error occurred")


if __name__ == "__main__":
    main()
