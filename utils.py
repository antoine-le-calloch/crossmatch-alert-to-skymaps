import io
import math
import fastavro

from datetime import datetime, timedelta
from astropy.time import Time

RED = "\033[31m"
YELLOW = "\033[33m"
ENDC = "\033[0m"

# BOOM stores ZTF flux as mag2flux(mag, 23.9) * 1e9 (see boom/src/alert/ztf.rs),
# so the inverse uses an effective AB zero point of 23.9 + 2.5*log10(1e9) = 46.4.
BOOM_ZTF_FLUX_ZP = 23.9 + 22.5
_FACTOR = 2.5 / math.log(10)


def log(message):
    print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - {message}")


def flux_to_mag(flux, zp=BOOM_ZTF_FLUX_ZP):
    """Convert flux to AB magnitude."""
    mag = -2.5 * math.log10(flux) + zp
    return mag


def flux_err_to_mag_error(flux, flux_err):
    """Convert flux error to AB magnitude error."""
    return _FACTOR * (flux_err / flux)


def flux_err_to_limiting_mag(flux_err, zp=BOOM_ZTF_FLUX_ZP):
    """5-sigma AB limiting magnitude from flux_err."""
    return -2.5 * math.log10(5.0 * flux_err) + zp


def fallback(hours=0, seconds=0, date_format=None):
    """Get a fallback date by subtracting a specified amount of time from the current UTC time.

    Parameters
    ----------
    hours : int, optional
        The number of hours to subtract from the current time (default is 0).
    seconds : int, optional
        The number of seconds to subtract from the current time (default is 0).
    date_format : str, optional
        The format in which to return the date (default is None, which returns a datetime object).
        If "iso", returns an ISO 8601 string.
        If "mjd", returns the Modified Julian Date.
        If "jd", returns the Julian Date.

    Returns
    -------
    datetime or str or float
        The fallback date in the specified format.
    """
    date = datetime.utcnow() - timedelta(hours=hours, seconds=seconds)
    if date_format == "iso":
        return date.isoformat()
    if date_format == "mjd":
        return Time(date).mjd
    if date_format == "jd":
        return Time(date).jd
    return date


def read_avro(msg):
    """
    Reads an Avro record from a Kafka message.

    Parameters
    ----------
    msg : Kafka message
        The Kafka message containing the Avro record.

    Returns
    -------
    dict or None
        The first record found in the Avro message, or None if no records are found.
    """
    bytes_io = io.BytesIO(msg.value())  # Get the message value as bytes
    bytes_io.seek(0)
    for record in fastavro.reader(bytes_io):
        return record  # Return the first record found
    return None  # Return None if no records are found or if an error occurs


def get_filtered_photometry(alert, snr_threshold, first_detection_fallback):
    """
    Filter the photometry of an alert to keep only the last non-detection and all detections,
    while also checking if the object is too old based on the SNR threshold and the first detection fallback.

    Parameters
    ----------
    alert : dict
        The alert containing photometry data.
    snr_threshold : float
        The SNR threshold to consider an object as too old.
    first_detection_fallback : float
        The Julian Date fallback for the first detection
    Returns
    -------
    list
        A list of photometry points that includes the last non-detection and all detections, or None if too old.
    """
    last_non_detection = []
    filtered_photometry = []
    for phot in reversed(alert.get("photometry", [])):  # From the most recent to the oldest
        if phot["programid"] != 1 or phot["origin"] == "ForcedPhot" or (phot["flux"] and phot["flux"] < 0):
            continue # Skip non-public ZTF alerts, forced photometry, and negative fluxes

        if phot["flux"] and phot["flux_err"]:  # If it's a detection
            last_non_detection = []  # Reset last non-detection as we found a detection
            filtered_photometry.append(phot)
            if phot["flux"] / phot["flux_err"] >= snr_threshold and phot["jd"] < first_detection_fallback:
                # If at least one detection with SNR >= snr_threshold is older than first_detection_fallback, consider the object as too old and skip it
                return None
        elif not last_non_detection:
            last_non_detection = [phot]

    # Keep the last non-detection and all detections
    return last_non_detection + list(reversed(filtered_photometry))