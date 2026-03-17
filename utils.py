import io
import fastavro
import numpy as np
import astropy.units as u

from datetime import datetime, timedelta
from astropy.time import Time
from mocpy import MOC
from astropy.io import fits
from astropy_healpix import HEALPix

RED = "\033[31m"
YELLOW = "\033[33m"
ENDC = "\033[0m"

def log(message):
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - {message}")


def fallback(hours=0, seconds=0, date_format=None):
    date = datetime.utcnow() - timedelta(hours=hours, seconds=seconds)
    if date_format == "iso":
        return date.isoformat()
    if date_format == "mjd":
        return Time(date).mjd
    if date_format == "jd":
        return Time(date).jd
    return date


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


def get_skymaps(skyportal, cumulative_probability, gcn_events):
    """Get all skymaps from SkyPortal since a given date. For each skymap,
    compute the MOC corresponding to the cumulative_probability threshold.

    Parameters
    ----------
    skyportal : SkyPortal
        An instance of the SkyPortal API client.
    gcn_events : list of dict
        A list of GCN event filtered and populated with the most recent localization > 1000 sq. deg.
    cumulative_probability : float
        The cumulative probability threshold for the MOC. Only tiles contributing
        to this cumulative probability will be included in the MOC.

    Returns
    -------
    results : dict
        A dictionary where keys are GCN event IDs and values are tuples of (skymap dateobs, alias, MOC).
    """
    results = {}
    for gcn_event in gcn_events:
        skymap = gcn_event["localization"]
        bytesIO_file = skyportal.download_localization(skymap["dateobs"], skymap["localization_name"])
        moc = get_moc_from_fits(bytesIO_file, cumulative_probability)
        if gcn_event.get("aliases"):
            alias = gcn_event.get("aliases")[0].split('#')[-1]
        else:
            alias = f"No aliases"
        results[gcn_event["id"]] = (skymap["dateobs"], alias, moc)

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


def read_avro(msg):
    """
    Reads an Avro record from a Kafka message.

    Args:
        msg: The message object containing the Avro data.

    Returns:
        The first record found in the Avro message, or None if no records are found.
    """

    bytes_io = io.BytesIO(msg.value())  # Get the message value as bytes
    bytes_io.seek(0)
    for record in fastavro.reader(bytes_io):
        return record  # Return the first record found
    return None  # Return None if no records are found or if an error occurs


def get_snr(phot):
  if phot["flux"] is None or not phot["flux_err"]:
      return None
  return phot["flux"] / phot["flux_err"]


def get_filtered_photometry(alert, snr_threshold, first_detection_fallback):
    """
    Filter the photometry of an alert to keep only the last non-detection and all detections,
    while also checking if the object is too old based on the SNR threshold and the first detection fallback.

    Args:
        alert (dict): The alert containing photometry data.
        snr_threshold (float): The SNR threshold to consider an object as too old.
        first_detection_fallback (float): The Julian Date fallback for the first detection
    Returns:
        list: A list of photometry points that includes the last non-detection and all detections, or None if too old.
    """
    last_non_detection = []
    filtered_photometry = []
    for phot in reversed(alert.get("photometry", [])):  # From the most recent to the oldest
        if phot["origin"] == "ForcedPhot":
            continue

        snr = get_snr(phot)
        if snr:  # If it's a detection
            last_non_detection = []  # Reset last non-detection as we found a detection
            filtered_photometry.append(phot)
            if snr >= snr_threshold and phot["jd"] < first_detection_fallback:
                # If at least one detection with SNR >= {snr_threshold} is older than first_detection_fallback, consider the object as too old and skip it
                return None
        elif not last_non_detection:
            last_non_detection = [phot]

    # Keep the last non-detection and all detections
    return last_non_detection + list(reversed(filtered_photometry))