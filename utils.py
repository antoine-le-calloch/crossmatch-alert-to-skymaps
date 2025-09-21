import time
import numpy as np
import astropy.units as u

from datetime import datetime, timedelta
from astropy.time import Time
from mocpy import MOC
from astropy.io import fits


def get_moc_from_fits(bytes, cumulative_probability):
    """Extract MOC from a FITS file containing a HEALPix localization map.
    Parameters
    ----------
    bytes : io.BytesIO
        A BytesIO object containing the FITS file data.
    cumulative_probability : float
        The cumulative probability threshold for the MOC.
    Returns
    -------
    moc : MOC
        The MOC corresponding to the cumulative_probability threshold.
    """
    with fits.open(bytes) as hdul:
        data = hdul[1].data
    uniq = data["UNIQ"]
    probdensity = data["PROBDENSITY"]

    # let's convert the probability density into a probability
    orders = (np.log2(uniq // 4)) // 2
    area = 4 * np.pi / np.array([MOC.n_cells(int(order)) for order in orders]) * u.sr
    prob = probdensity * area
    return MOC.from_valued_healpix_cells(uniq, prob, 29, cumul_to=cumulative_probability)


def get_skymaps(skyportal, cumulative_probability, fallback):
    """Get all skymaps from SkyPortal since a given date. For each localization,
    compute the MOC corresponding to the cumulative_probability threshold.

    Parameters
    ----------
    skyportal : SkyPortal
        An instance of the SkyPortal API client.
    fallback : int
        The number of days to look back for GCN events.
    cumulative_probability : float
        The cumulative probability threshold for the MOC. Only tiles contributing
        to this cumulative probability will be included in the MOC.

    Returns
    -------
    results : list of tuples
        A list of tuples, each containing a localization ID and its corresponding MOC.
    """
    gcn_events = skyportal.get_gcn_events(datetime.utcnow() - timedelta(days=fallback))
    if not gcn_events:
        return []

    results = []
    for gcn_event in gcn_events:
        if not gcn_event.get("localizations"):
            continue
        last_localization = gcn_event.get("localizations")[0]
        bytesIO_file = skyportal.download_localization(last_localization["dateobs"], last_localization["localization_name"])
        moc = get_moc_from_fits(bytesIO_file, cumulative_probability)
        results.append((last_localization["id"], moc))

    return results

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
    if len(objs) > 0:
        print(f"Found {len(results)} valid objects on {len(objs)} in {time.time() - start_time:.2f} seconds")
    return results