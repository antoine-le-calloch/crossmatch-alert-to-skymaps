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
        '$schema': 'https://gcn.nasa.gov/schema/v6.3.0/gcn/notices/boom/alert.schema.json',
        "alert_datetime": Time.now().isot + "Z",
        "mission": "Boom",
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
                        "ref_type": skymap.type,
                        "ref_instrument": skymap.instrument,
                        "ref_ID": skymap.id,
                    } for skymap in matching_skymaps.values()],
                }
            ],
            "photometry": [{
                "event_name": obj["objectId"],
                "observation_start": Time(p["jd"], format="jd", precision=3).isot + "Z",
                "telescope": "Palomar 1.2m Oschin",
                "instrument": "ZTF",
                "filter": p["band"],
                **(
                    {
                        "mag": round(flux_to_mag(p["flux"]), 2),
                        "mag_error": round(flux_err_to_mag_error(p["flux"], p["flux_err"]), 2),
                    } if p["flux"] and p["flux_err"] else {}
                ),
                "mag_system": "AB",
                "limiting_mag": round(flux_err_to_limiting_mag(p["flux_err"]), 2),
            } for p in obj["filtered_photometry"]]
        },
    }
    return payload


def send_to_gcn(obj, matching_skymaps, notify_slack=True):
    gcn_payload = prepare_gcn_payload(obj, matching_skymaps)
    if notify_slack:
        send_to_slack(obj, matching_skymaps, gcn_payload)