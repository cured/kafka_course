import json
import time

from confluent_kafka import Consumer
import logging
from opensearchpy import OpenSearch
from opensearchpy.exceptions import RequestError
from opensearchpy import helpers
import settings


logger = logging.getLogger("opensearch")
logging.basicConfig(level=logging.DEBUG)


def extractId(record):
    loaded = json.loads(record)
    return loaded["meta"]["id"]


class OpensearchConsumer:
    topic = "wikimedia.recentchange"
    opensearch_index = settings.opensearch_index_name

    def __init__(self):
        self.consumer = Consumer({
            "bootstrap.servers": "127.0.0.1:9092",
            "partition.assignment.strategy": "cooperative-sticky",
            "group.id": "consumer-opensearch-demo",
            "enable.auto.commit": "false"
        })
        self.consumer.subscribe([self.topic])

        auth = (settings.opensearch_user, settings.opensearch_pass)
        try:
            self.opensearch_client = OpenSearch(
                hosts=[{
                    "host": settings.opensearch_host,
                    "port": settings.opensearch_port,
                }],
                http_auth=auth,
                http_compress=True,
                use_ssl=True,
                verify_certs=False,
                ssl_assert_hostname=False,
                ssl_show_warn=False
            )
            self.init_index()
        except Exception as e:
            self.consumer.close()

    def init_index(self):
        r = self.opensearch_client.indices.exists(self.opensearch_index)
        if not r:
            self.opensearch_client.indices.create(self.opensearch_index)

    def run(self):
        self._run()

    def _run(self):
        try:
            records = []
            while True:
                record = self.consumer.poll()
                if record is None or not record.value():
                    continue

                if len(records) < settings.batch_size:
                    records.append(record)
                    if not(len(records) % 20):
                        logger.debug(f"Records in bulk {len(records)}")
                    continue

                processed = 0
                ok, response = helpers.bulk(
                    client=self.opensearch_client,
                    index=self.opensearch_index,
                    actions=self._generate_actions(records)
                )

                logger.debug(response)

                logger.debug(f"Next {len(records)} records are processed")
                r = self.consumer.commit(asynchronous=False)
                logger.debug(r)
                logger.debug(f"Committed")
                records = []
                time.sleep(5)
        except KeyboardInterrupt:
            logger.debug('%% Aborted by user\n')
            self.consumer.close()

    def _generate_actions(self, records):
        l = []
        for r in records:
            id = extractId(r.value())
            doc = {
                "_source": json.loads(r.value()),
                "id": id,
                "_index": self.opensearch_index
            }
            logger.debug(f"in generate actions {doc}")
            l.append(doc)

        return l


def main():
    c = OpensearchConsumer()
    if c.consumer:
        c.run()


if __name__ == "__main__":
    main()
