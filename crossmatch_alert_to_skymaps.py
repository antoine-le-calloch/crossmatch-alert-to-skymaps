import os
import time

import astropy.units as u
from datetime import datetime, timedelta

from astropy.time import Time
from dotenv import load_dotenv

from api import SkyPortal
from utils import get_skymaps

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
skyportal_api_key = os.getenv("SKYPORTAL_API_KEY")
allocation_id = os.getenv("ALLOCATION_ID")
group_ids_to_listen = os.getenv("GROUP_IDS_TO_LISTEN")

fallback_in_days = 2

def is_obj_in_localizations(ra, dec, localizations):
    """
    Check if an object is within any of the provided localizations.
    Parameters
    ----------
    ra : float
        Right Ascension of the object in degrees.
    dec : float
        Declination of the object in degrees.
    localizations : list of tuples
        List of tuples where each tuple contains a localization ID and its corresponding MOC.

    Returns
    -------
    list
        All localization IDs that contain the object.
    """
    matching_localizations = [
        loc_id
        for loc_id, moc in localizations
        if moc.contains_lonlat(ra * u.deg, dec * u.deg)
    ]
    return matching_localizations

def get_valid_obj(skyportal, payload, snr_threshold, fallback):
    """
    Retrieve objects from SkyPortal and filter them based on the first detection.

    Parameters
    ----------
    skyportal : SkyPortal
        An instance of the SkyPortal API client.
    payload : dict
        The payload to use for the get_objects API call.
    snr_threshold : float
        The signal-to-noise ratio threshold for the first detection.
    fallback : int
        The number of days to look back for the first detection.

    Returns
    -------
    list
        All objects that meet the criteria.

    """
    fallback_mjd = Time(datetime.utcnow() - timedelta(days=fallback)).mjd
    start_time = time.time()
    objs = skyportal.get_objects(payload)
    results = []
    for obj in objs:
        # Keep the object if its first detection with SNR ≥ threshold occurs after the fallback date
        for phot in sorted(obj.get("photometry", []), key=lambda p: p.get("mjd")):
            if phot["flux"] and phot["fluxerr"] and phot["flux"] / phot["fluxerr"] >= snr_threshold:
                if phot["mjd"] >= fallback_mjd:
                    results.append(obj)
                    break
    print(f"Found {len(results)} valid objects on {len(objs)} in {time.time() - start_time:.2f} seconds")
    return results

def crossmatch_alert_to_skymaps():
    skyportal = SkyPortal(instance=skyportal_url, token=skyportal_api_key)
    fallback_date = datetime.utcnow() - timedelta(days=fallback_in_days)
    latest_gcn_date_obs = fallback_date
    latest_obj_refresh = fallback_date
    cumulative_probability = 0.95
    snr_threshold = 5.0
    skymaps = None

    while True:
        # Check if new GCNs have been observed since the last observation
        new_latest_gcn_events = skyportal.get_gcn_events(latest_gcn_date_obs + timedelta(seconds=1))
        if skymaps is None or new_latest_gcn_events: # If new GCNs, fetch again skymaps from the last 2 days
            print(f"New GCNs found, fetching skymaps")
            start_time = time.time()
            skymaps = get_skymaps(skyportal, datetime.utcnow() - timedelta(days=fallback_in_days), cumulative_probability)
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
                print("No new objects to crossmatch. Waiting...")
        else:
            print("No skymaps available. Waiting...")
        time.sleep(20)

if __name__ == "__main__":
    try:
        crossmatch_alert_to_skymaps()
    except Exception as e:
        print(f"Error in crossmatch_alert_to_skymaps service: {e}")
        raise e