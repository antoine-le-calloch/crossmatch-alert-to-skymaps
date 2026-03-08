from astropy.time import Time

from slack import send_to_slack

telescope_by_instrument_id = {}

def setup_telescope_list(skyportal):
    global telescope_by_instrument_id
    instruments = skyportal.get_instruments()
    if not instruments:
        raise ValueError("No instruments retrieved from SkyPortal.")
    telescope_by_instrument_id = {
        instrument["id"]: instrument["telescope"]["name"]
        for instrument in instruments
    }

def prepare_gcn_payload(obj, matching_skymaps):
    payload = {
        "title": f"Optical alert {obj['objectId']}",
        "data": {
            "targets": [
                {
                    "event_name": obj["objectId"],
                    "ra": obj["ra"],
                    "dec": obj["dec"],
                    "classification_scores": {
                        classification["classifier"]: classification["score"]
                        for classification in obj.get("classifications", [])
                    },
                    "gcn_crossmatch":  [{
                        "ref_type": "ref_type",
                        "ref_instrument": "ref_instrument",
                        "ref_ID": alias
                    } for _, alias, _ in matching_skymaps],
                }
            ],
            "photometry": [{
                "event_name": obj["objectId"],
                "observation_start": Time(p["jd"], format="jd").iso,
                "telescope": "Palomar 1.2m Oschin",
                "instrument": "ZTF",
                "filter": [p["band"]],
                "brightness": p["flux"],
                "brightness_error": p["flux_err"],
                "brightness_unit": "ab",
                # "limiting_brightness": p["limiting_mag"],
                "limiting_brightness_unit": "ab"
            } for p in obj["filtered_photometry"]]
        },
    }
    return payload

def send_to_gcn(obj, matching_skymaps, notify_slack=True):
    gcn_payload = prepare_gcn_payload(obj, matching_skymaps)
    if notify_slack:
        send_to_slack(obj, matching_skymaps, gcn_payload)