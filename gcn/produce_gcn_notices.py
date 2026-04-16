import json
import urllib.request

from astropy.time import Time
from gcn_kafka import Producer
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from utils.gcn import CLIENT_ID, CLIENT_SECRET, DOMAIN, SCHEMA, TOPIC, HEARTBEAT_TOPIC
from utils.logger import log, RED, ENDC

gcn_producer = Producer(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    domain=DOMAIN
)


def _retrieve_remote_schema(uri):
    with urllib.request.urlopen(uri) as response:
        return Resource.from_contents(json.load(response))


try:
    with urllib.request.urlopen(SCHEMA) as response:
        schema = json.load(response)
    registry = Registry(retrieve=_retrieve_remote_schema)
    validator = Draft202012Validator(schema, registry=registry)

except Exception as e:
    log(f"{RED}Failed to build GCN notice payload validator: {e}{ENDC}")
    log(f"{RED}GCN notice payloads will not be validated against the schema before being sent to Kafka.{ENDC}")
    validator = None


def produce_to_gcn(data, topic=TOPIC, validate=True):
    if validate and validator:
        validator.validate(data)
    # JSON data converted to byte string format
    data = json.dumps(data).encode()
    gcn_producer.produce(topic, data)
    gcn_producer.flush()


def produce_gcn_heartbeat():
    heartbeat_payload = {
        "timestamp": Time.now().isot + "Z",
        "status": "alive"
    }
    produce_to_gcn(HEARTBEAT_TOPIC, heartbeat_payload, validate=False)