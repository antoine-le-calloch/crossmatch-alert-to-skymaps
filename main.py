import os
import time
import argparse
import traceback

from astropy.time import Time
from dotenv import load_dotenv

from gcn.produce_gcn_notices import produce_gcn_notice
from utils.api import SkyPortal, APIError
from utils.logger import log, RED, ENDC
from utils.skymap import get_skymap
from utils.kafka import read_avro, boom_consumer
from utils.converter import fallback
from utils.gcn import prepare_gcn_payload
from utils.slack import send_to_slack

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
skyportal_api_key = os.getenv("SKYPORTAL_API_KEY")
boom_filters = os.getenv("BOOM_KAFKA_FILTERS").split(",")

GCN = 24*6  # hours for GCN fallback
FIRST_DETECTION = 24*5  # hours for first detection fallback
SLEEP_TIME = 20 # seconds between each loop


def send_to_gcn(obj, matching_skymaps, notify_slack=True):
    gcn_payload = prepare_gcn_payload(obj, matching_skymaps)
    produce_gcn_notice(gcn_payload)

    if notify_slack:
        send_to_slack(obj, matching_skymaps, gcn_payload)


def get_filtered_photometry(alert, snr_threshold, first_detection_fallback):
    """
    Filter the photometry of an alert to keep only the last non-detection and all detections,
    while also checking if the object is too old based on the SNR threshold and the first detection fallback.

    Parameters
    ----------
    alert : dict
        The alert containing photometry data.
    snr_threshold : float
        The SNR threshold to consider an object as too old.
    first_detection_fallback : float
        The Julian Date fallback for the first detection
    Returns
    -------
    list
        A list of photometry points that includes the last non-detection and all detections, or None if too old.
    """
    last_non_detection = []
    filtered_photometry = []
    for phot in reversed(alert.get("photometry", [])):  # From the most recent to the oldest
        if phot["programid"] != 1 or phot["origin"] == "ForcedPhot" or not phot["flux_err"] or (phot["flux"] and phot["flux"] < 0):
            continue # Skip non-public ZTF alerts, forced photometry, no flux_err and negative fluxes

        if phot["flux"] and phot["flux_err"]:  # If it's a detection
            last_non_detection = []  # Reset last non-detection as we found a detection
            filtered_photometry.append(phot)
            if phot["flux"] / phot["flux_err"] >= snr_threshold and phot["jd"] < first_detection_fallback:
                # If at least one detection with SNR >= snr_threshold is older than first_detection_fallback, consider the object as too old and skip it
                return None
        elif not last_non_detection:
            last_non_detection = [phot]

    # Keep the last non-detection and all detections
    return last_non_detection + list(reversed(filtered_photometry))


def crossmatch_alert_to_skymaps():
    skyportal = SkyPortal(instance=skyportal_url, token=skyportal_api_key)
    cumulative_probability = 0.95
    snr_threshold = 5.0
    processed_alerts = {}  # {objectId: {"skymaps": set((dateobs,created_at)), "first_detection_jd": float}}
    skymaps = {} # {dateobs: Skymap}
    timer = None

    consumer = boom_consumer()
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
                    if not event.get("aliases") or not any("#" in alias for alias in event["aliases"]):
                        continue # Filter out GCN events with bad or no aliases

                    event["localization"] = next(
                        (loc for loc in event.get("localizations", [])
                         if any(tag["text"] == "< 1000 sq. deg." for tag in loc.get("tags", []))),
                        None
                    )
                    if event["localization"] is None:
                        continue
                    elif event["dateobs"] not in skymaps:
                        new_gcn_events.append(event)
                    elif skymaps[event["dateobs"]].created_at < event["localization"]["created_at"]:
                        # If the localization is newer than the one we have for that dateobs, we should recompute this event
                        new_gcn_events.append(event)

                for gcn_event in new_gcn_events:
                    skymaps[gcn_event["dateobs"]] = get_skymap(skyportal, cumulative_probability, gcn_event)
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

                matching_skymaps = {}
                for dateobs, skymap in skymaps.items():
                    if not filtered_photometry[0]["jd"] <= Time(dateobs).jd <= filtered_photometry[1]["jd"]:
                        continue # Skymap is not between the last non-detection and the first detection

                    if obj_id in processed_alerts and (dateobs, skymap.created_at) in processed_alerts[obj_id].get("skymaps", set()):
                        log(f"Skipping already processed skymap {dateobs} for object {obj_id}")
                        continue # This skymap has already been processed for this object

                    if skymap.contains(alert["ra"], alert["dec"]):
                        # If the object is in the skymap, add it to the matching_skymaps dictionary
                        matching_skymaps[dateobs] = skymap

                if matching_skymaps:
                    # Process the crossmatch results here (e.g., send to GCN, log, etc.)
                    skymaps_string = ", ".join(skymap.name for skymap in matching_skymaps.values())
                    log(f"{obj_id} matches the following skymaps: {skymaps_string}")
                    alert["filtered_photometry"] = filtered_photometry
                    send_to_gcn(alert, matching_skymaps, notify_slack=True)

                    # Add the object and matching skymaps to processed_alerts to avoid re-processing
                    dateobs_created_at_tuple = set((dateobs, skymap.created_at) for dateobs, skymap in matching_skymaps.items())
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
        from utils.slack import delete_all_bot_messages
        delete_all_bot_messages()

    crossmatch_alert_to_skymaps()