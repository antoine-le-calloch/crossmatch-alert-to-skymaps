import os
import time

from datetime import datetime, timedelta
from dotenv import load_dotenv
from api import SkyPortal
from utils import get_skymaps, get_valid_obj, is_obj_in_localizations

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
skyportal_api_key = os.getenv("SKYPORTAL_API_KEY")
allocation_id = os.getenv("ALLOCATION_ID")
group_ids_to_listen = os.getenv("GROUP_IDS_TO_LISTEN")

fallback_in_days = 2

def crossmatch_alert_to_skymaps():
    skyportal = SkyPortal(instance=skyportal_url, token=skyportal_api_key)
    latest_gcn_date_obs = datetime.utcnow() - timedelta(days=fallback_in_days)
    latest_obj_refresh = datetime.utcnow() - timedelta(hours=3)
    cumulative_probability = 0.95
    snr_threshold = 5.0
    skymaps = None

    while True:
        print(f"\n{datetime.utcnow()}")
        # Check if new GCNs have been observed since the last observation
        new_latest_gcn_events = skyportal.get_gcn_events(latest_gcn_date_obs + timedelta(seconds=1))
        if skymaps is None or new_latest_gcn_events: # If new GCNs, fetch again skymaps from the last 2 days
            print(f"New GCNs found, fetching skymaps")
            start_time = time.time()
            skymaps = get_skymaps(skyportal, cumulative_probability, fallback_in_days)
            print(f"Fetching {len(skymaps)} skymaps and creating MOCs took {time.time() - start_time:.2f} seconds")

            if new_latest_gcn_events:
                latest_gcn_date_obs = datetime.fromisoformat(new_latest_gcn_events[0].get('dateobs'))

        # Retrieve objects created after last refresh time
        if skymaps:
            get_objects_payload = {
                "startDate": latest_obj_refresh.isoformat(),
                "includePhotometry": True,
            }
            if group_ids_to_listen:
                get_objects_payload["groupIDs"] = group_ids_to_listen

            latest_obj_refresh=datetime.utcnow() # Update the refresh time before the query
            objs = get_valid_obj(skyportal, get_objects_payload, snr_threshold, fallback_in_days)
            crossmatches = []
            start_time = time.time()
            for obj in objs:
                matching_localizations = is_obj_in_localizations(obj["ra"], obj["dec"], skymaps)
                if matching_localizations:
                    crossmatches.append({"obj": obj, "localizations": matching_localizations})
                    # TODO: Do something with the object, e.g., publish somewhere

            if len(objs) > 0:
                print(f"Found {len(crossmatches)} crossmatches in {time.time() - start_time:.2f} seconds")
        else:
            print("No skymaps available. Waiting...")
        time.sleep(20)

if __name__ == "__main__":
    try:
        crossmatch_alert_to_skymaps()
    except Exception as e:
        print(f"Error in crossmatch_alert_to_skymaps service: {e}")
        raise e