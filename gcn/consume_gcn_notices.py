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
        if not topic_filter or topic_filter in topic:
            log(f"        {topic}")
    log("")


def gcn_notices_consumer(topics=None, offset_reset='latest'):
    """
    Consume GCN notices from specified topics and print them in a human-readable format.

    Parameters
    ----------
    topics : list of str, optional
        List of topics to subscribe to. If None, defaults to [TOPIC, HEARTBEAT_TOPIC].
    offset_reset : str, optional
        Offset reset policy for the consumer. Defaults to 'latest'. Other options include 'earliest' and 'none'.
    """
    consumer = Consumer(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        domain=DOMAIN,
        config={
            'auto.offset.reset': offset_reset
        }
    )

    topic_list = consumer.list_topics().topics
    topics_to_consume = []
    for topic in topics or [TOPIC, HEARTBEAT_TOPIC]:
        if not any(available_topic == topic for available_topic in topic_list):
            log(f"{YELLOW}Topic '{topic}' not found in available topics. Skipping.{ENDC}")
        else:
            topics_to_consume.append(topic)

    consumer.subscribe(topics_to_consume)
    log(f"Subscribed to topic: {topics_to_consume}")
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