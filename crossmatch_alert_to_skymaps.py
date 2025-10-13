import os
import time
import requests
import argparse

from astropy.time import Time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from api import SkyPortal
from utils import get_skymaps, get_and_process_valid_obj, is_obj_in_skymaps, get_new_skymaps_for_processed_obj
from gcn_notices import send_to_gcn, setup_telescope_list

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
skyportal_api_key = os.getenv("SKYPORTAL_API_KEY")
allocation_id = os.getenv("ALLOCATION_ID")
group_ids_to_listen = os.getenv("GROUP_IDS_TO_LISTEN")

GCN = 48  # hours for GCN fallback
ALERT = 12  # hours for alert fallback
FIRST_DETECTION = 48  # hours for first detection fallback
SLEEP_TIME = 20 # seconds between each loop

def log(message):
    print(f"{datetime.utcnow()} - {message}")

def fallback(hours=0, seconds=0, date_format=None):
    date = datetime.utcnow() - timedelta(hours=hours, seconds=seconds)
    if date_format == "iso":
        return date.isoformat()
    if date_format == "mjd":
        return Time(date).mjd
    return date


def crossmatch_alert_to_skymaps():
    skyportal = SkyPortal(instance=skyportal_url, token=skyportal_api_key)
    latest_gcn_date_obs = fallback(GCN)
    latest_obj_refresh = fallback(ALERT)
    cumulative_probability = 0.95
    snr_threshold = 5.0
    skymaps = None
    no_new_object_found = False
    setup_telescope_list(skyportal)
    is_first_run = True

    while True:
        try:
            # Check if SkyPortal is available
            skyportal.ping()

            # Check if new GCNs have been observed since the last observation
            new_latest_gcn_events = skyportal.get_gcn_events(latest_gcn_date_obs + timedelta(seconds=1))

            if new_latest_gcn_events: # If new GCNs, fetch again skymaps from the GCN fallback
                start_time = time.time()
                skymaps = get_skymaps(skyportal, cumulative_probability, fallback(GCN))
                log(f"Fetching {len(skymaps)} skymaps and creating MOCs took {time.time() - start_time:.2f} seconds")
                latest_gcn_date_obs = datetime.fromisoformat(new_latest_gcn_events[0].get('dateobs'))

            elif skymaps: # If no new GCNs, check for expired localizations and remove them
                gcn_fallback_iso = fallback(GCN, date_format="iso")
                # Iterate in reverse to get older items first
                for dateobs, alias, moc in reversed(skymaps.copy()):
                    if dateobs >= gcn_fallback_iso:
                        break
                    log(f"Removed expired localization {dateobs}")
                    skymaps.remove((dateobs, alias, moc))

            # Retrieve objects created after last refresh time
            if skymaps:
                get_objects_payload = {
                    "startDate": max(latest_obj_refresh, fallback(ALERT)).isoformat(),
                }
                if group_ids_to_listen:
                    get_objects_payload["groupIDs"] = group_ids_to_listen

                latest_obj_refresh=datetime.utcnow() # Update the refresh time before the query
                start_time = time.time()
                objs, nb_objs_before_filtering = get_and_process_valid_obj(
                    skyportal,
                    get_objects_payload,
                    snr_threshold,
                    fallback(FIRST_DETECTION, date_format="mjd")
                )
                if objs:
                    log(f"Found {len(objs)} new valid objects on {nb_objs_before_filtering} in {time.time() - start_time:.2f} seconds")
                nb_crossmatches = 0
                start_time = time.time()
                for obj in objs:
                    new_skymaps = get_new_skymaps_for_processed_obj(
                        obj,
                        skymaps,
                        fallback(seconds=SLEEP_TIME,date_format="mjd"),
                        is_first_run,
                    )
                    matching_skymaps = is_obj_in_skymaps(obj["ra"], obj["dec"], new_skymaps)
                    if matching_skymaps:
                        # Perform actions for each crossmatched object
                        send_to_gcn(obj, matching_skymaps, notify_slack=True)
                        nb_crossmatches += 1
                if objs:
                    log(f"Found {nb_crossmatches} crossmatches in {time.time() - start_time:.2f} seconds")
                    no_new_object_found = False
                elif not no_new_object_found: # Only log once when no new objects are found
                    log(f"No new objects found. Waiting...")
                    no_new_object_found = True
            else:
                log("No skymaps available. Waiting...")

        except requests.exceptions.Timeout:
            log(f"SkyPortal instance is not available.")
        except Exception as e:
            log(e)

        if is_first_run: is_first_run = False
        time.sleep(SLEEP_TIME)

if __name__ == "__main__":
    # --- CLI arguments ---
    parser = argparse.ArgumentParser(description="Crossmatch alerts with GCN skymaps.")
    parser.add_argument(
        "--alert-fallback",
        "-af",
        type=int,
        default=12,
        help="Alert fallback in hours (default: 12).",
    )
    args = parser.parse_args()
    ALERT = args.alert_fallback or ALERT

    crossmatch_alert_to_skymaps()