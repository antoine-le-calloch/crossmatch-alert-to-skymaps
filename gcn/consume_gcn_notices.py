import json

from gcn_kafka import Consumer

from utils.gcn import CLIENT_ID, CLIENT_SECRET, DOMAIN, TOPIC
from utils.logger import log, RED, ENDC


def list_gcn_topics(topic_filter=None):
    consumer = Consumer(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        domain=DOMAIN
    )
    log(f"Listing available {topic_filter or ''} GCN topics:")
    for topic in consumer.list_topics().topics:
        if topic_filter in topic:
            log(f"        {topic}")
    log("")


def gcn_notices_consumer(topics=None):
    gcn_notices_config = {
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        domain=DOMAIN,
        config=gcn_notices_config
    )

    topics = topics or [TOPIC]
    consumer.subscribe(topics)
    log(f"Subscribed to topic: {topics}")
    while True:
        for message in consumer.consume(timeout=1):
            if message.error():
                log(f"{RED}{message.error()}{ENDC}")
                continue
            log(f'topic={message.topic()}, offset={message.offset()}')

            try:
                value = message.value().decode("utf-8")
                data = json.loads(value)
                print(json.dumps(data, indent=2))
                print("\n----------------------------------\n")

            except Exception as e:
                log(f"{RED}Failed to parse JSON: {e}{ENDC}")
                log(f"{RED}Raw message value: {message.value()}{ENDC}")