import json

from gcn_kafka import Consumer

from utils.gcn import CLIENT_ID, CLIENT_SECRET, DOMAIN, TOPIC, HEARTBEAT_TOPIC
from utils.logger import log, RED, YELLOW, ENDC


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


def gcn_notices_consumer(topics=None, offset_reset='latest'):
    consumer = Consumer(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        domain=DOMAIN,
        config={
            'auto.offset.reset': offset_reset
        }
    )

    if topics is None:
        topic_list = consumer.list_topics().topics
        if not any(topic == TOPIC for topic in topic_list):
            log(f"{RED}Error: topic '{TOPIC}' not found in available topics. Please check your configuration.{ENDC}")
            return
        if not any(topic == HEARTBEAT_TOPIC for topic in topic_list):
            log(f"{YELLOW}Heartbeat topic '{HEARTBEAT_TOPIC}' not found in available topics.{ENDC}")
            topics = [TOPIC]
        else:
            topics = [TOPIC, HEARTBEAT_TOPIC]

    consumer.subscribe(topics)
    log(f"Subscribed to topic: {topics}")
    while True:
        for message in consumer.consume(timeout=1):
            if message.error():
                log(f"{RED}{message.error()}{ENDC}")
                continue
            print("\n----------------------------------\n")
            log(f'topic={message.topic()}, offset={message.offset()}')

            try:
                value = message.value().decode("utf-8")
                data = json.loads(value)
                print(json.dumps(data, indent=2))

            except Exception as e:
                log(f"{RED}Failed to parse JSON: {e}{ENDC}")
                log(f"{RED}Raw message value: {message.value()}{ENDC}")