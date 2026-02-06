import os
import time
import argparse
import traceback

from astropy.time import Time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from api import SkyPortal, APIError
from utils import get_skymaps, get_and_process_valid_obj, is_obj_in_skymaps, get_new_skymaps_for_processed_obj, log, RED, ENDC
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

def fallback(hours=0, seconds=0, date_format=None):
    date = datetime.utcnow() - timedelta(hours=hours, seconds=seconds)
    if date_format == "iso":
        return date.isoformat()
    if date_format == "mjd":
        return Time(date).mjd
    return date


def crossmatch_alert_to_skymaps():
    skyportal = SkyPortal(instance=skyportal_url, token=skyportal_api_key)
    setup_telescope_list(skyportal)
    latest_obj_refresh = fallback(ALERT)
    cumulative_probability = 0.95
    snr_threshold = 5.0
    skymaps = {}

    # Flags to control logging
    no_skymaps = False
    no_new_object = False
    is_first_run = True

    while True:
        try:
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

            elif skymaps: # If no new GCNs, check for expired localizations and remove them
                gcn_fallback_iso = fallback(GCN, date_format="iso")
                expired = [event_id for event_id, (dateobs, alias, moc) in skymaps.items() if dateobs < gcn_fallback_iso]
                for event_id in expired:
                    log(f"Removed expired skymap {skymaps[event_id][0]}")
                    del skymaps[event_id]

            # Retrieve objects created after last refresh time
            if skymaps:
                get_objects_payload = {
                    "startDate": max(latest_obj_refresh, fallback(ALERT)).isoformat(),
                }
                if group_ids_to_listen:
                    get_objects_payload["groupIDs"] = group_ids_to_listen

                refresh_time=datetime.utcnow()
                start_time = time.time()
                objs, nb_objs_before_filtering = get_and_process_valid_obj(
                    skyportal,
                    get_objects_payload,
                    snr_threshold,
                    fallback(FIRST_DETECTION, date_format="mjd")
                )
                latest_obj_refresh = refresh_time # Update the refresh time after successful query
                if objs:
                    log(f"Found {len(objs)} new valid objects on {nb_objs_before_filtering} in {time.time() - start_time:.2f} seconds")
                nb_crossmatches = 0
                start_time = time.time()
                for obj in objs:
                    skymaps_tuples = list(skymaps.values())
                    new_skymaps_tuples = get_new_skymaps_for_processed_obj(
                        obj,
                        skymaps_tuples,
                        fallback(seconds=SLEEP_TIME,date_format="mjd"),
                        is_first_run,
                    )
                    matching_skymaps = is_obj_in_skymaps(obj["ra"], obj["dec"], new_skymaps_tuples)
                    if matching_skymaps:
                        # Perform actions for each crossmatched object
                        send_to_gcn(obj, matching_skymaps, notify_slack=True)
                        nb_crossmatches += 1
                if objs:
                    log(f"Found {nb_crossmatches} crossmatches in {time.time() - start_time:.2f} seconds")
                    no_new_object = False
                elif not no_new_object: # Only log once when no new objects are found
                    log(f"No new objects found. Waiting...")
                    log("               .")
                    log("               .")
                    log("               .")
                    no_new_object = True
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

        if is_first_run: is_first_run = False
        time.sleep(SLEEP_TIME)

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
        "--keep-slack",
        "-ks",
        action="store_true",
        help="Keep existing Slack messages.",
    )
    args = parser.parse_args()
    ALERT = args.alert_fallback or ALERT
    if not args.keep_slack:
        from slack import delete_all_bot_messages
        delete_all_bot_messages()

    crossmatch_alert_to_skymaps()