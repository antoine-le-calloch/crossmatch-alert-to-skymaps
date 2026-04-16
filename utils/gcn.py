import os

from dotenv import load_dotenv
from astropy.time import Time

from utils.logger import log, RED, YELLOW, GREEN, ENDC
from utils.converter import flux_to_mag, flux_err_to_mag_error, flux_err_to_limiting_mag

load_dotenv()

SCHEMA = "https://gcn.nasa.gov/schema/v6.3.0/gcn/notices/boom/alert.schema.json"

TESTING_MODE = os.getenv("GCN_KAFKA_TESTING_MODE", "true")
if TESTING_MODE.lower() == "false":
    log(f"{GREEN}GCN Kafka is running in PRODUCTION MODE using production server and credentials.{ENDC}")
    TESTING_MODE = False
else:
    if TESTING_MODE.lower() != "true":
        log(f"{RED}Invalid value for GCN_KAFKA_TESTING_MODE: {TESTING_MODE}. Defaulting to TESTING MODE.{ENDC}")
    log(f"{YELLOW}GCN Kafka is running in TESTING MODE using test server and credentials.{ENDC}")
    TESTING_MODE = True

CLIENT_ID = os.getenv("GCN_KAFKA_USERNAME" if not TESTING_MODE else "GCN_KAFKA_TEST_USERNAME")
CLIENT_SECRET = os.getenv("GCN_KAFKA_PASSWORD" if not TESTING_MODE else "GCN_KAFKA_TEST_PASSWORD")
DOMAIN = os.getenv("GCN_KAFKA_SERVER" if not TESTING_MODE else "GCN_KAFKA_TEST_SERVER")
TOPIC = os.getenv("GCN_KAFKA_TOPIC" if not TESTING_MODE else "GCN_KAFKA_TEST_TOPIC")
HEARTBEAT_TOPIC = f"{TOPIC}.heartbeat"


def prepare_gcn_payload(obj, matching_skymaps):
    payload = {
        '$schema': SCHEMA,
        "alert_datetime": Time.now().isot + "Z",
        "mission": "Boom",
        "data": {
            "targets": [
                {
                    "event_name": obj["objectId"],
                    "ra": obj["ra"],
                    "dec": obj["dec"],
                    "classification_scores": {
                        classification["classifier"]: classification["score"]
                        for classification in obj.get("classifications", [])
                    },
                    "gcn_crossmatch":  [{
                        "ref_type": skymap.type,
                        "ref_instrument": skymap.instrument,
                        "ref_ID": skymap.id,
                    } for skymap in matching_skymaps.values()],
                }
            ],
            "photometry": [{
                "event_name": obj["objectId"],
                "observation_start": Time(p["jd"], format="jd", precision=3).isot + "Z",
                "telescope": "Palomar 1.2m Oschin",
                "instrument": "ZTF",
                "filter": p["band"],
                **(
                    {
                        "mag": round(flux_to_mag(p["flux"]), 2),
                        "mag_error": round(flux_err_to_mag_error(p["flux"], p["flux_err"]), 2),
                    } if p["flux"] and p["flux_err"] else {}
                ),
                "mag_system": "AB",
                "limiting_mag": round(flux_err_to_limiting_mag(p["flux_err"]), 2),
            } for p in obj["filtered_photometry"]]
        },
    }
    return payload