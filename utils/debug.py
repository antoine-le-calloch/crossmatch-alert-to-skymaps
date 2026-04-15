import matplotlib.pyplot as plt
import matplotlib.image as mpimg

from utils.logger import log
from utils.skymap import plot_object_on_skymap

def display_skymaps(obj, skymaps, plot=False):
    """Display information about the skymaps that match the given object and optionally plot them.

    Parameters
    ----------
    obj : dict
        A dictionary containing the object details, including "objectId", "ra", and "dec".
    skymaps : dict
        A dictionary of skymaps, where the keys are dateobs and the values are Skymap objects.
    plot : bool, optional
        Whether to plot the skymaps using matplotlib. Default is False.
    """
    ra, dec = obj["ra"], obj["dec"]
    log(f"Displaying {len(skymaps)} skymap(s) for {obj['objectId']} (ra={ra:.5f}, dec={dec:.5f}):")
    for dateobs, skymap in skymaps.items():
        is_in = skymap.contains(ra, dec)
        is_match = f"{'  ' if is_in else 'NO'} MATCH"
        log(f"Type: {skymap.type} | Instrument: {skymap.instrument} | Id: {skymap.id} | [{is_match}] {skymap.alias} dateobs={dateobs}")

        if plot:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(mpimg.imread(plot_object_on_skymap(obj, skymap.moc)))
            ax.axis("off")
            ax.set_title(f"[{is_match}] {skymap.alias} — {dateobs}")
            plt.show()
