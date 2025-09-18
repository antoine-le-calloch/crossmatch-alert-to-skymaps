import os
import time

import astropy.units as u
from datetime import datetime, timedelta
from dotenv import load_dotenv

from api import SkyPortal
from utils import get_skymaps

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
skyportal_api_key = os.getenv("SKYPORTAL_API_KEY")
allocation_id = os.getenv("ALLOCATION_ID")
group_ids_to_listen = os.getenv("GROUP_IDS_TO_LISTEN")

def is_obj_in_localizations(ra, dec, localizations):
    matching_localizations = [
        loc_id
        for loc_id, moc in localizations
        if moc.contains_lonlat(ra * u.deg, dec * u.deg)
    ]
    return matching_localizations

def crossmatch_alert_to_skymaps():
    # Start by checking GCNs and objects from the last 2 days
    two_days_ago = datetime.utcnow() - timedelta(days=2)
    latest_gcn_date_obs = two_days_ago
    latest_obj_refresh = two_days_ago
    cumulative_probability = 0.95
    snr_threshold = 5.0  # Minimum SNR for photometry to consider an object, TODO: implement this filter
    skymaps = None
    skyportal = SkyPortal(instance=skyportal_url, token=skyportal_api_key)

    while True:
        # Check if new GCNs with tag "GW" have been observed since the last observation
        new_latest_gcn_events = skyportal.get_gcn_events(
            {"startDate": latest_gcn_date_obs + timedelta(seconds=1), "gcnTagKeep": "GW", "excludeNoticeContent": True}
        )
        if skymaps is None or new_latest_gcn_events: # If new GCNs, fetch again skymaps from the last 2 days
            print(f"New GCNs found, fetching skymaps")
            start_time = time.time()
            skymaps = get_skymaps(skyportal, datetime.utcnow() - timedelta(days=2), cumulative_probability)
            print(f"Fetching {len(skymaps)} skymaps and creating MOCs took {time.time() - start_time:.2f} seconds")

            if new_latest_gcn_events:
                latest_gcn_date_obs = datetime.fromisoformat(new_latest_gcn_events[0].get('dateobs'))

        # Retrieve objects created after last refresh time
        payload = {
            "startDate": latest_obj_refresh,
            "firstDetectionAfter": datetime.utcnow() - timedelta(days=3),
        }
        if group_ids_to_listen:
            payload["groupIDs"] = group_ids_to_listen
        latest_obj_refresh=datetime.utcnow() # Update the refresh time before the query
        objs = skyportal.get_objects(payload)
        start_time = time.time()
        crossmatches = []
        for obj in objs:
            if skymaps and is_obj_in_localizations(obj["ra"], obj["dec"], skymaps):
                crossmatches.append(obj)
                # TODO: Do something with the object, e.g., publish somewhere

        if len(objs) > 0:
            print(f"Crossmatching {len(objs)} objects took {time.time() - start_time:.2f} seconds")
            print(f"Found {len(crossmatches)} objects in skymaps out of {len(objs)} new objects")
        else:
            print("No new objects to crossmatch. Waiting...")

        time.sleep(20)

if __name__ == "__main__":
    try:
        crossmatch_alert_to_skymaps()
    except Exception as e:
        print(f"Error in crossmatch_alert_to_skymaps service: {e}")
        raise e