import time
import numpy as np
import astropy.units as u
from astropy.time import Time

from mocpy import MOC
from astropy.io import fits


def get_moc_from_fits(bytes, cumulative_probability):
    """Extract MOC from a FITS file containing a HEALPix skymap map.
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
    """Get all skymaps from SkyPortal since a given date. For each skymap,
    compute the MOC corresponding to the cumulative_probability threshold.

    Parameters
    ----------
    skyportal : SkyPortal
        An instance of the SkyPortal API client.
    fallback : datetime.datetime
        The starting date and time to filter GCN events from.
    cumulative_probability : float
        The cumulative probability threshold for the MOC. Only tiles contributing
        to this cumulative probability will be included in the MOC.

    Returns
    -------
    results : list of tuples
        A list of tuples, each containing a skymap dateobs and its corresponding MOC.
    """
    gcn_events = skyportal.get_gcn_events(fallback)
    if not gcn_events:
        return []

    results = []
    for gcn_event in gcn_events:
        if not gcn_event.get("localizations"):
            continue
        skymap = gcn_event.get("localizations")[0] # Take the most recent skymap
        bytesIO_file = skyportal.download_localization(skymap["dateobs"], skymap["localization_name"])
        moc = get_moc_from_fits(bytesIO_file, cumulative_probability)
        results.append((skymap["dateobs"], moc))

    return results

def is_obj_in_skymaps(ra, dec, skymaps):
    """
    Check if an object is within any of the provided skymaps (MOCs).
    Parameters
    ----------
    ra : float
        Right Ascension of the object in degrees.
    dec : float
        Declination of the object in degrees.
    skymaps : list of tuples
        List of tuples where each tuple contains a skymap dateobs and its corresponding MOC.

    Returns
    -------
    list
        A list of tuples containing the dateobs and MOC of skymaps that contain the object.
    """
    matching_skymaps = [
        (dateobs, moc)
        for dateobs, moc in skymaps
        if moc.contains_lonlat(ra * u.deg, dec * u.deg)
    ]
    return matching_skymaps

def get_valid_obj(skyportal, payload, snr_threshold, first_detection_fallback):
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
    first_detection_fallback : int
        First detection fallback in mjd.

    Returns
    -------
    list
        All objects that meet the criteria.

    """
    objs = skyportal.get_objects(payload)
    results = []
    for obj in objs:
        filtered_photometry = [] # Photometry that meets the SNR threshold
        for phot in sorted(obj.get("photometry", []), key=lambda p: p.get("mjd"), reverse=True):
            if phot["flux"] and phot["fluxerr"] and phot["flux"] / phot["fluxerr"] >= snr_threshold:
                filtered_photometry.append(phot)
                if phot["mjd"] < first_detection_fallback:
                    break
        else: # If no detection before the fallback, keep the object
            obj["filtered_photometry"] = filtered_photometry
            results.append(obj)
    return results


def get_new_skymaps_for_processed_obj(obj, skymaps, last_processed_mjd):
    """
    If the object has already been processed
    (i.e., has more than one filtered photometry point in less than sleeping time),
    return only the skymaps that are newer than the last processed photometry point.
    Parameters
    ----------
    obj : dict
        The object containing photometry data.
    skymaps : list of tuples
        List of tuples where each tuple contains a skymap dateobs and its corresponding MOC
    last_processed_mjd : int
        The last processed time in mjd.

    Returns
    -------
    list
        A list of tuples containing the dateobs and MOC of skymaps that are newer than the last processed photometry point.

    """
    # Remove the last photometry point as it is the one that triggered the current processing
    photometry = obj.get("filtered_photometry", [])[:-1]
    if len(photometry) < 1:
        return skymaps

    for phot in reversed(photometry):
        if phot["mjd"] < last_processed_mjd:
            last_processed_alert_mjd = phot["mjd"]
            break
    else: # If no alert have been already processed, return all skymaps
        return skymaps

    return [(dateobs, moc) for dateobs, moc in skymaps if Time(dateobs).mjd >= last_processed_alert_mjd]