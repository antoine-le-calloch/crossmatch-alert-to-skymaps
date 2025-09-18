import numpy as np
import astropy.units as u

from mocpy import MOC
from astropy.io import fits


def get_moc_from_fits(bytesIO_file, cumulative_probability):
    with fits.open(bytesIO_file) as hdul:
        data = hdul[1].data
    uniq = data["UNIQ"]
    probdensity = data["PROBDENSITY"]

    # let's convert the probability density into a probability
    orders = (np.log2(uniq // 4)) // 2
    area = 4 * np.pi / np.array([MOC.n_cells(int(order)) for order in orders]) * u.sr
    prob = probdensity * area
    return MOC.from_valued_healpix_cells(uniq, prob, 29, cumul_to=cumulative_probability)


def get_skymaps(skyportal, dateobs, cumulative_probability):
    """Get all skymaps between dateobs and now. For each localization,
    compute the MOC corresponding to the cumulative_probability threshold.

    Parameters
    ----------
    skyportal : SkyPortal
        An instance of the SkyPortal API client.
    dateobs : datetime.datetime
        The starting date and time to filter skymaps from. Only skymaps
        with a date greater than or equal to this will be returned.
    cumulative_probability : float
        The cumulative probability threshold for the MOC. Only tiles contributing
        to this cumulative probability will be included in the MOC.

    Returns
    -------
    results : list of tuples
        A list of tuples, each containing a localization ID and its corresponding MOC.
    """
    gcn_events = skyportal.get_gcn_events({"startDate": dateobs, "gcnTagKeep": "GW", "excludeNoticeContent": True})
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