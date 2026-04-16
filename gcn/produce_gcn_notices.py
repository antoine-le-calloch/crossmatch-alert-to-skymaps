import json
import urllib.request

from astropy.time import Time
from gcn_kafka import Producer
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from utils.gcn import CLIENT_ID, CLIENT_SECRET, DOMAIN, SCHEMA, TOPIC, HEARTBEAT_TOPIC


gcn_producer = Producer(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    domain=DOMAIN
)


def produce_to_gcn(topic, data):
    # JSON data converted to byte string format
    data = json.dumps(data).encode()
    gcn_producer.produce(topic, data)
    gcn_producer.flush()


def _retrieve_remote_schema(uri):
    with urllib.request.urlopen(uri) as response:
        return Resource.from_contents(json.load(response))


def _build_validator():
    with urllib.request.urlopen(SCHEMA) as response:
        schema = json.load(response)
    registry = Registry(retrieve=_retrieve_remote_schema)
    return Draft202012Validator(schema, registry=registry)


_validator = _build_validator()


def validate_gcn_payload(payload):
    _validator.validate(payload)


def produce_gcn_heartbeat():
    heartbeat_payload = {
        "timestamp": Time.now().isot + "Z",
        "status": "alive"
    }
    produce_to_gcn(HEARTBEAT_TOPIC, heartbeat_payload)


def produce_gcn_notice(json_payload):
    validate_gcn_payload(json_payload)
    produce_to_gcn(TOPIC, json_payload)