import json
import urllib.request

from gcn_kafka import Producer
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from utils.gcn import CLIENT_ID, CLIENT_SECRET, DOMAIN, SCHEMA, TOPIC

producer = Producer(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    domain=DOMAIN
)


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


def produce_gcn_notice(json_payload):
    validate_gcn_payload(json_payload)

    # JSON data converted to byte string format
    data = json.dumps(json_payload).encode()
    producer.produce(TOPIC, data)
    producer.flush()