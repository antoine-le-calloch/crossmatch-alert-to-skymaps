import os
import time
import argparse
import traceback

from astropy.time import Time
from dotenv import load_dotenv
from confluent_kafka import Consumer
from api import SkyPortal, APIError
from utils import (
    get_filtered_photometry,
    is_obj_in_skymaps,
    get_skymap,
    read_avro,
    fallback,
    log,
    RED,
    ENDC
)
from gcn_notices import send_to_gcn, setup_telescope_list

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
skyportal_api_key = os.getenv("SKYPORTAL_API_KEY")
boom_filters = os.getenv("BOOM_KAFKA_FILTERS").split(",")

GCN = 24*6  # hours for GCN fallback
FIRST_DETECTION = 24*5  # hours for first detection fallback
SLEEP_TIME = 20 # seconds between each loop

config = {
    'bootstrap.servers': os.getenv("BOOM_KAFKA_SERVERS"),
    'group.id': f'umn_boom_kafka_consumer_group_{int(time.time())}',
    'auto.offset.reset': 'earliest',
    "enable.auto.commit": False,
}
if os.getenv("BOOM_KAFKA_USERNAME") and os.getenv("BOOM_KAFKA_PASSWORD"):
    config.update({
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": os.getenv("BOOM_KAFKA_USERNAME"),
        "sasl.password": os.getenv("BOOM_KAFKA_PASSWORD"),
    })
else:
    config["security.protocol"] = "PLAINTEXT"

def crossmatch_alert_to_skymaps():
    skyportal = SkyPortal(instance=skyportal_url, token=skyportal_api_key)
    setup_telescope_list(skyportal)
    cumulative_probability = 0.95
    snr_threshold = 5.0
    processed_alerts = {}  # {objectId: {"skymaps": set((dateobs,created_at)), "first_detection_jd": float}}
    skymaps = {} # {dateobs: {"alias": str, "moc": MOC, "created_at": str}}
    timer = None

    # Subscribe to Boom Kafka topics
    consumer = Consumer(config)
    topic = os.getenv("BOOM_KAFKA_TOPIC")
    consumer.subscribe([topic])
    log(f"Subscribed to topic: {topic}")
    log(f"Listening for alerts passing the following Boom filters: {boom_filters}")

    # Flags
    no_skymaps = False

    while True:
        try:
            # only check that every SLEEP_TIME seconds to avoid hitting the API
            if not timer or time.time() - timer >= SLEEP_TIME:
                timer = time.time() # reset timer

                # Check if SkyPortal is available
                skyportal.ping()

                # Check for new GCN events or new localizations for existing events with "< 1000 sq. deg." tag
                new_gcn_events = []
                for event in skyportal.get_gcn_events(fallback(GCN)):
                    event["localization"] = next(
                        (loc for loc in event.get("localizations", [])
                         if any(tag["text"] == "< 1000 sq. deg." for tag in loc.get("tags", []))),
                        None
                    )
                    if event["localization"] is None:
                        continue
                    elif event["dateobs"] not in skymaps:
                        new_gcn_events.append(event)
                    elif skymaps[event["dateobs"]].get("created_at") < event["localization"]["created_at"]:
                        # If the localization is newer than the one we have for that dateobs, we should recompute this event
                        new_gcn_events.append(event)

                for gcn_event in new_gcn_events:
                    moc = get_skymap(skyportal, cumulative_probability, gcn_event["localization"])
                    skymaps[gcn_event["dateobs"]] = {
                        "alias": gcn_event["aliases"][0] if gcn_event.get("aliases") else "No aliases",
                        "moc": moc,
                        "created_at": gcn_event["localization"]["created_at"],
                    }
                if new_gcn_events:
                    log(f"Fetching {len(new_gcn_events)} skymaps and creating MOCs")

                # Clean up old skymaps (GCN events older than fallback)
                gcn_fallback_iso = fallback(GCN, date_format="iso")
                for dateobs in list(skymaps.keys()):
                    if dateobs < gcn_fallback_iso:
                        log(f"Removed expired skymap {dateobs} from skymaps")
                        del skymaps[dateobs]

                first_detection_fallback_jd = fallback(FIRST_DETECTION, date_format="jd")
                for obj_id, info in list(processed_alerts.items()):
                    if info["first_detection_jd"] < first_detection_fallback_jd:
                        log(f"Removed expired object {obj_id} from processed_alerts")
                        del processed_alerts[obj_id]

            # Consume new alerts passing a set of filters from Boom Kafka and crossmatch them with available skymaps
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                continue
            if msg.error():
                log(f"Consumer error: {msg.error()}")
                continue

            if skymaps:
                alert = read_avro(msg)

                if not any(filter.get("filter_name") in boom_filters for filter in alert.get("filters", [])):
                    continue
                obj_id = alert["objectId"]

                filtered_photometry = get_filtered_photometry(alert, snr_threshold, fallback(FIRST_DETECTION, date_format="jd"))
                if not filtered_photometry or len(filtered_photometry) < 2:
                    continue # The First detection is too old or the alert doesn't have any non-detection

                filtered_skymaps = {}
                for dateobs, skymap in skymaps.items():
                    if not filtered_photometry[0]["jd"] <= Time(dateobs).jd <= filtered_photometry[1]["jd"]:
                        continue # Skymap is not between the last non-detection and the first detection
                    if obj_id in processed_alerts and (dateobs, skymap["created_at"]) in processed_alerts[obj_id].get("skymaps", set()):
                        log(f"Skipping already processed skymap {dateobs} for object {obj_id}")
                    else:
                        filtered_skymaps[dateobs] = skymap

                matching_skymaps = is_obj_in_skymaps(alert["ra"], alert["dec"], filtered_skymaps)
                if matching_skymaps:
                    # Process the crossmatch results here (e.g., send to GCN, log, etc.)
                    skymaps_string = ", ".join([f"{skymap.get('alias')}/{skymap.get('created_at')}" for skymap in matching_skymaps.values()])
                    log(f"{obj_id} matches the following skymaps: {skymaps_string}")
                    alert["filtered_photometry"] = filtered_photometry
                    send_to_gcn(alert, matching_skymaps, notify_slack=True)

                    # Add the object and matching skymaps to processed_alerts to avoid re-processing
                    dateobs_created_at_tuple = set((dateobs, skymap["created_at"]) for dateobs, skymap in matching_skymaps.items())
                    if obj_id not in processed_alerts:
                        processed_alerts[obj_id] = {
                            "skymaps": dateobs_created_at_tuple,
                            "first_detection_jd": filtered_photometry[1]["jd"],
                        }
                    else:
                        processed_alerts[obj_id]["skymaps"].update(dateobs_created_at_tuple)

            elif not no_skymaps:  # Only log once when no skymaps are available
                log("No skymaps available. Waiting...")
                log("               .")
                log("               .")
                log("               .")
                no_skymaps = True

        except APIError as e:
            log(e)
        except Exception:
            log(f"{RED}An error occurred:{ENDC}")
            traceback.print_exc()

if __name__ == "__main__":
    # --- CLI arguments ---
    parser = argparse.ArgumentParser(
        description="Crossmatch alerts with GCN skymaps.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--gcn",
        "-g",
        type=int,
        default=GCN,
        help="GCN fallback in hours.",
    )
    parser.add_argument(
        "--detection",
        "-d",
        type=int,
        default=FIRST_DETECTION,
        help="First detection fallback in hours.",
    )
    parser.add_argument(
        "--sleep-time",
        "-s",
        type=int,
        default=SLEEP_TIME,
        help="Time in seconds to wait between each check for new GCN events.",
    )
    parser.add_argument(
        "--clean-slack",
        "-cs",
        action="store_true",
        help="Whether to delete all current bot messages in the Slack channel before starting the script.",
    )
    args = parser.parse_args()
    GCN = args.gcn
    FIRST_DETECTION = args.detection
    SLEEP_TIME = args.sleep_time
    if args.clean_slack:
        from slack import delete_all_bot_messages
        delete_all_bot_messages()

    crossmatch_alert_to_skymaps()