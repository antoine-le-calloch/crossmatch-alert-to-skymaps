
telescope_by_instrument_id = {}

def setup_telescope_list(skyportal):
    global telescope_by_instrument_id
    telescope_by_instrument_id = {
        instrument["id"]: instrument["telescope"]["name"]
        for instrument in skyportal.get_instruments()
    }

def prepare_gcn_payload(obj, matching_skymaps):
    payload = {
        "title": f"SkyPortal report for {obj['id']}",
        "data": {
            "targets": [
                {
                    "name": obj["id"],
                    "ra": obj["ra"],
                    "dec": obj["dec"],
                    "classifications": [{
                        "classification": classification["classification"],
                        "probability": classification["probability"],
                    } for classification in obj.get("classifications", [])],
                    "gcn_crossmatch":  [alias for _, alias, _ in matching_skymaps],
                }
            ],
            "photometry": [{
                "target_name": p["obj_id"],
                "date_obs": p["mjd"],
                "telescope": telescope_by_instrument_id[p["instrument_id"]],
                "instrument": p["instrument_name"],
                "bandpass": p["filter"],
                "brightness": p["mag"],
                "brightness_error": p["magerr"],
                "unit": "ab",
                "limiting_brightness": p["limiting_mag"],
                "limiting_brightness_unit": "ab",
            } for p in obj["filtered_photometry"]]
        },
    }
    return payload

def send_to_gcn(obj, matching_skymaps):
    payload = prepare_gcn_payload(obj, matching_skymaps)