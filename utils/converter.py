import math

from astropy.time import Time
from datetime import datetime, timedelta, UTC

# BOOM stores ZTF flux as mag2flux(mag, 23.9) * 1e9 (see boom/src/alert/ztf.rs),
# so we use AB zero point of 23.9 + 2.5*log10(1e9) = 46.4.
BOOM_ZTF_FLUX_ZP = 23.9 + 22.5
_FACTOR = 2.5 / math.log(10)

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
    date = datetime.now(UTC) - timedelta(hours=hours, seconds=seconds)
    if date_format == "iso":
        return date.isoformat()
    if date_format == "mjd":
        return Time(date).mjd
    if date_format == "jd":
        return Time(date).jd
    return date