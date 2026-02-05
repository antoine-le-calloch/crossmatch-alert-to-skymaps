import numpy as np
import astropy.units as u

from datetime import datetime
from astropy.time import Time
from mocpy import MOC
from astropy.io import fits
from astropy_healpix import HEALPix

def log(message):
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - {message}")

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
        columns = [col.name for col in hdul[1].columns]
        header = hdul[1].header

    if "UNIQ" in columns:
        # Multi-order format
        uniq = data["UNIQ"]
        probdensity = data["PROBDENSITY"]
        orders = (np.log2(uniq // 4)) // 2
        area = 4 * np.pi / np.array([MOC.n_cells(int(order)) for order in orders]) * u.sr
        prob = probdensity * area
    else:
        # Flat HEALPix format
        prob_col = next(c for c in columns if c in ("PROB", "PROBABILITY", "PROBDENSITY"))
        prob = np.ravel(data[prob_col])
        npix = len(prob)
        nside = int(np.sqrt(npix / 12))
        order = int(np.log2(nside))

        # Convert from RING to NESTED ordering if needed (UNIQ scheme uses NESTED)
        ordering = header.get("ORDERING", "NESTED").upper()
        if ordering == "RING":
            ring_hp = HEALPix(nside=nside, order="ring")
            nested_hp = HEALPix(nside=nside, order="nested")
            lon, lat = ring_hp.healpix_to_lonlat(np.arange(npix))
            nested_indices = nested_hp.lonlat_to_healpix(lon, lat)
            reordered = np.empty(npix)
            reordered[nested_indices] = prob
            prob = reordered

        indices = np.arange(npix)
        uniq = 4 * (4 ** order) + indices

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
        A list of tuples, each containing a skymap dateobs, its alias, and the corresponding MOC.
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
        if gcn_event.get("aliases"):
            alias = gcn_event.get("aliases")[0].split('#')[-1]
        else:
            alias = f"No aliases"
        results.append((skymap["dateobs"], alias, moc))

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
        List of tuples where each tuple contains a skymap dateobs, its alias, and the corresponding MOC.

    Returns
    -------
    list
        A list of tuples containing the dateobs, alias, and the MOC of skymaps that contain the object.
    """
    matching_skymaps = [
        (dateobs, alias, moc)
        for dateobs, alias, moc in skymaps
        if moc.contains_lonlat(ra * u.deg, dec * u.deg)
    ]
    return matching_skymaps

def get_and_process_valid_obj(skyportal, payload, snr_threshold, first_detection_fallback):
    """
    Retrieve objects and photometry from SkyPortal, filter them based on snr and the first detection.
    And update each object with the filtered photometry.

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
    results : list
        A list of objects with their filtered photometry.
    total_objs : int
        The total number of objects retrieved before filtering.

    """
    objs = skyportal.get_objects(payload)
    results = []
    for obj in objs:
        photometry = skyportal.get_object_photometry(obj["id"])
        last_non_detection = []
        filtered_photometry = []
        for phot in reversed(photometry):
            if phot["snr"]: # If it's a detection
                last_non_detection = [] # Reset last non-detection as we found a detection
                filtered_photometry.append(phot)
                if phot["snr"] >= snr_threshold and phot["mjd"] < first_detection_fallback:
                    break
            elif not last_non_detection:
                last_non_detection = [phot]
        else: # If no detection before the fallback, keep the object
            results.append({
                **obj,
                "filtered_photometry": last_non_detection + list(reversed(filtered_photometry))
            })
    return results, len(objs)

def get_new_skymaps_for_processed_obj(obj, skymaps, last_processed_mjd, is_first_run=False):
    """
    If the object has already been processed
    (i.e., has more than one filtered photometry point in less than sleeping time),
    return only the skymaps that are newer than the last processed photometry point.
    Parameters
    ----------
    obj : dict
        The object containing photometry data.
    skymaps : list of tuples
        List of tuples where each tuple contains a skymap dateobs, its alias, and the corresponding MOC.
    last_processed_mjd : int
        The last processed time in mjd.
    is_first_run : bool, optional
        Whether this is the first run (default is False). If True, all skymaps are returned.

    Returns
    -------
    list
        A list of tuples containing the dateobs, alias, and MOC of skymaps that are newer than the last processed photometry point.

    """
    # Remove the last photometry point as it is the one that triggered the current processing
    photometry = obj.get("filtered_photometry", [])[:-1]
    if len(photometry) < 1 or is_first_run:
        return skymaps

    for phot in reversed(photometry):
        if phot["mjd"] < last_processed_mjd:
            last_processed_alert_mjd = phot["mjd"]
            break
    else: # If no alert have been already processed, return all skymaps
        return skymaps

    return [(dateobs, alias, moc) for dateobs, alias, moc in skymaps if Time(dateobs).mjd >= last_processed_alert_mjd]