
def prepare_gcn_payload(obj, matching_skymaps):
    payload_photometry = []
    for p in obj.get("photometry", []):
        payload_photometry.append(
            {
                "target_name": p["obj_id"],
                "date_obs": p["mjd"],
                # "telescope": p.instrument.telescope.name,
                "instrument": p["instrument"].get("name"),
                "bandpass": p.get("filter"),
                "flux": p.get("flux"),
                "flux_error": p.get("fluxerr"),
                # "brightness": p.get("mag"),
                # "brightness_error": p["magerr"],
                # "unit": "AB mag",
                # "limiting_brightness": p.get("limiting_mag"),
                # "limiting_brightness_unit": "AB mag",
            }
        )

    payload = {
        "title": f"SkyPortal report for {obj['id']}",
        "data": {
            "targets": [
                {
                    "name": obj["id"],
                    "ra": obj["ra"],
                    "dec": obj["dec"],
                    "classifications": obj.get("classifications", []),
                    "gcn_crossmatch":  matching_skymaps,
                }
            ],
            "photometry": payload_photometry,
        },
    }
    return payload

def send_to_gcn(obj, matching_skymaps):
    payload = prepare_gcn_payload(obj, matching_skymaps)