import os
import time
import argparse
import traceback

from datetime import datetime

from astropy.time import Time
from dotenv import load_dotenv
from confluent_kafka import Consumer
from api import SkyPortal, APIError
from utils import get_skymaps, is_obj_in_skymaps, fallback, log, RED, ENDC, read_avro, get_snr
from gcn_notices import send_to_gcn, setup_telescope_list

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
skyportal_api_key = os.getenv("SKYPORTAL_API_KEY")
allocation_id = os.getenv("ALLOCATION_ID")
group_ids_to_listen = os.getenv("GROUP_IDS_TO_LISTEN")

GCN = 48  # hours for GCN fallback
ALERT = 12  # hours for alert fallback
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

consumer = Consumer(config)
topic = os.getenv("BOOM_KAFKA_TOPIC")
consumer.subscribe([topic])
log(f"Subscribed to topic: {topic}")

def crossmatch_alert_to_skymaps():
    skyportal = SkyPortal(instance=skyportal_url, token=skyportal_api_key)
    setup_telescope_list(skyportal)
    cumulative_probability = 0.95
    snr_threshold = 5.0
    skymaps = {}
    timer = None

    # Flags
    no_skymaps = False
    no_new_alert = False
    jd_of_first_processed_alert = None

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

            # Consume new alerts passing a given filter from Boom Kafka and crossmatch with skymaps
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                if not no_new_alert: # Only log once when no new alerts are found
                    no_new_alert = True
                    log(f"No new alerts available")
                    log("               .")
                    log("               .")
                    log("               .")
                continue
            if msg.error():
                log(f"Consumer error: {msg.error()}")
                continue
            no_new_alert = False

            if not jd_of_first_processed_alert:
                jd_of_first_processed_alert = Time(datetime.utcnow()).jd
            if skymaps:
                alert = read_avro(msg)

                if not any(filter.get("filter_name") == os.getenv("BOOM_KAFKA_FILTER") for filter in alert.get("filters", [])):
                    continue

                last_non_detection = []
                filtered_photometry = []
                too_old_object = False
                for phot in reversed(alert.get("photometry", [])): # From the most recent to the oldest
                    if phot["origin"] == "ForcedPhot":
                        continue

                    snr = get_snr(phot)
                    if snr: # If it's a detection
                        last_non_detection = []  # Reset last non-detection as we found a detection
                        filtered_photometry.append(phot)
                        if snr >= snr_threshold and phot["jd"] < fallback(FIRST_DETECTION, date_format="jd"):
                            too_old_object = True
                            break
                    elif not last_non_detection:
                        last_non_detection = [phot]

                if too_old_object:
                    # log(f"Object {alert['objectId']} is too old (at least one detection with SNR >= {snr_threshold} is older than {FIRST_DETECTION} hours). Skipping.")
                    continue

                # Keep the last non-detection and all detections
                filtered_photometry = last_non_detection + list(reversed(filtered_photometry))
                # If the last photometry point (other than the last one which are the one who triggered the alert)
                # have already been processed (i.e. its jd is after the first processed alert of this code)
                # only keep new skymaps since the last processed alert
                if len(filtered_photometry) > 2 and jd_of_first_processed_alert < filtered_photometry[-2]["jd"]:
                    new_skymaps_tuples = [(dateobs, alias, moc) for dateobs, alias, moc in skymaps.values() if
                               Time(dateobs).jd >= filtered_photometry[-2]["jd"]]
                else:
                    new_skymaps_tuples = list(skymaps.values())

                matching_skymaps = is_obj_in_skymaps(alert["ra"], alert["dec"], new_skymaps_tuples)
                if matching_skymaps:
                    log(f"Alert {alert['objectId']} matches the following skymaps: {[alias for _, alias, _ in matching_skymaps]}")
                    # Perform actions for each crossmatched alert
                    alert["filtered_photometry"] = filtered_photometry
                    send_to_gcn(alert, matching_skymaps, notify_slack=True)

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
    parser = argparse.ArgumentParser(description="Crossmatch alerts with GCN skymaps.")
    parser.add_argument(
        "--alert-fallback",
        "-af",
        type=int,
        help="Alert fallback in hours (default: 12).",
    )
    parser.add_argument(
        "--clean-slack",
        "-cs",
        action="store_true",
        help="Whether to delete all current bot messages in the Slack channel before starting the script.",
    )
    args = parser.parse_args()
    ALERT = args.alert_fallback or ALERT
    if args.clean_slack:
        from slack import delete_all_bot_messages
        delete_all_bot_messages()

    crossmatch_alert_to_skymaps()