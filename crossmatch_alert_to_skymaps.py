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
    get_skymaps,
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
allocation_id = os.getenv("ALLOCATION_ID")
group_ids_to_listen = os.getenv("GROUP_IDS_TO_LISTEN")
boom_filters = os.getenv("BOOM_KAFKA_FILTERS").split(",")

GCN = 48  # hours for GCN fallback
FIRST_DETECTION = 24  # hours for first detection fallback
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
    processed_alerts = {}  # {objectId: {"skymaps": set((alias, dateobs)), "first_detection_jd": float}}
    skymaps = {}
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
                    elif event["id"] not in skymaps:
                        new_gcn_events.append(event)
                    elif event["localization"]["dateobs"] > skymaps[event["id"]][0]:
                        new_gcn_events.append(event)

                if new_gcn_events:
                    start_time = time.time()
                    # Get skymaps for new GCN events
                    # Returns [{event_id: (dateobs, alias, moc)}, ...]
                    new_skymaps = get_skymaps(skyportal, cumulative_probability, new_gcn_events)
                    for event_id, skymap_tuple in new_skymaps.items():
                        skymaps[event_id] = skymap_tuple
                    log(f"Fetching {len(skymaps)} skymaps and creating MOCs took {time.time() - start_time:.2f} seconds")

                if skymaps: # If some skymaps are available, check for expired localizations and remove them
                    gcn_fallback_iso = fallback(GCN, date_format="iso")
                    expired = [event_id for event_id, (dateobs, alias, moc) in skymaps.items() if dateobs < gcn_fallback_iso]
                    for event_id in expired:
                        log(f"Removed expired skymap {skymaps[event_id][0]}")
                        del skymaps[event_id]

                # Clean up old objects from processed_alerts (first detection too old)
                expired_objects = [
                    obj_id for obj_id, info in processed_alerts.items()
                    if info["first_detection_jd"] < fallback(FIRST_DETECTION, date_format="jd")
                ]
                for obj_id in expired_objects:
                    log(f"Removed {obj_id} from processed alerts (first detection too old)")
                    del processed_alerts[obj_id]

            # Consume new alerts passing a given filter from Boom Kafka and crossmatch with skymaps
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

                # Only keep skymaps between last non-detection and first detection, excluding already processed ones
                filtered_skymaps_tuples = [
                    (dateobs, alias, moc) for dateobs, alias, moc in skymaps.values() if
                    filtered_photometry[0]["jd"] <= Time(dateobs).jd <= filtered_photometry[1]["jd"] + 1  # add 1 day margin for updated localizations
                    and (alias, dateobs) not in processed_alerts.get(obj_id, {}).get("skymaps", set())
                ]

                matching_skymaps = is_obj_in_skymaps(alert["ra"], alert["dec"], filtered_skymaps_tuples)
                if matching_skymaps:
                    # Process the crossmatch results here (e.g., send to GCN, log, etc.)
                    log(f"{obj_id} matches the following skymaps: {[alias for _, alias, _ in matching_skymaps]}")
                    alert["filtered_photometry"] = filtered_photometry
                    send_to_gcn(alert, matching_skymaps, notify_slack=True)

                    # Add the object and matching skymaps to processed_alerts to avoid re-processing
                    processed_alerts.setdefault(obj_id, {
                        "skymaps": set(), "first_detection_jd": filtered_photometry[1]["jd"]
                    })["skymaps"].update((alias, dateobs) for dateobs, alias, _ in matching_skymaps)

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
    if args.clean_slack:
        from slack import delete_all_bot_messages
        delete_all_bot_messages()

    crossmatch_alert_to_skymaps()