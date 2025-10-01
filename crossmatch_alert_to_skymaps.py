import os
import time

from datetime import datetime, timedelta
from dotenv import load_dotenv
from astropy.time import Time
from utils import get_skymaps, get_valid_obj, is_obj_in_skymaps
from api import SkyPortal
from gcn_notices import send_to_gcn
from slack import send_to_slack

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
skyportal_api_key = os.getenv("SKYPORTAL_API_KEY")
allocation_id = os.getenv("ALLOCATION_ID")
group_ids_to_listen = os.getenv("GROUP_IDS_TO_LISTEN")

GCN = 48  # hours for GCN fallback
ALERT = 2  # hours for alert fallback
FIRST_DETECTION = 48  # hours for first detection fallback

def fallback(hours=0, date_format=None):
    date = datetime.utcnow() - timedelta(hours=hours)
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

    while True:
        # Check if new GCNs have been observed since the last observation
        new_latest_gcn_events = skyportal.get_gcn_events(latest_gcn_date_obs + timedelta(seconds=1))

        if new_latest_gcn_events: # If new GCNs, fetch again skymaps from the GCN fallback
            print(f"New GCNs found, fetching skymaps")
            start_time = time.time()
            skymaps = get_skymaps(skyportal, cumulative_probability, fallback(GCN))
            print(f"Fetching {len(skymaps)} skymaps and creating MOCs took {time.time() - start_time:.2f} seconds")
            latest_gcn_date_obs = datetime.fromisoformat(new_latest_gcn_events[0].get('dateobs'))

        elif skymaps: # If no new GCNs, check for expired localizations and remove them
            gcn_fallback_iso = fallback(GCN, "iso")
            # Iterate in reverse to get older items first
            for dateobs, moc in reversed(skymaps.copy()):
                if dateobs >= gcn_fallback_iso:
                    break
                print(f"Removed expired localization {dateobs}")
                skymaps.remove((dateobs, moc))

        # Retrieve objects created after last refresh time
        if skymaps:
            get_objects_payload = {
                "startDate": max(latest_obj_refresh, fallback(ALERT)).isoformat(),
                "includePhotometry": True,
            }
            if group_ids_to_listen:
                get_objects_payload["groupIDs"] = group_ids_to_listen

            latest_obj_refresh=datetime.utcnow() # Update the refresh time before the query
            objs = get_valid_obj(
                skyportal,
                get_objects_payload,
                snr_threshold,
                fallback(FIRST_DETECTION, "mjd")
            )
            nb_crossmatches = 0
            start_time = time.time()
            for obj in objs:
                matching_skymaps = is_obj_in_skymaps(obj["ra"], obj["dec"], skymaps)
                if matching_skymaps:
                    # Perform actions for each crossmatched object
                    send_to_gcn(obj, matching_skymaps)
                    send_to_slack(obj, matching_skymaps)
                    nb_crossmatches += 1
            if objs:
                print(f"{datetime.utcnow()} Found {nb_crossmatches} crossmatches in {time.time() - start_time:.2f} seconds\n")
            else:
                print(f"No new objects found. Waiting...")
        else:
            print("No skymaps available. Waiting...")
        time.sleep(20)

if __name__ == "__main__":
    try:
        crossmatch_alert_to_skymaps()
    except Exception as e:
        print(f"Error in crossmatch_alert_to_skymaps service: {e}")
        raise e