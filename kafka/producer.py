from confluent_kafka import Producer


class ProducerDemo:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": "127.0.0.1:9092",
        })


def main():
    p = ProducerDemo()
    p.producer.produce(topic="demo_python", value="hello world")
    p.producer.flush()


if __name__ == "__main__":
    main()
