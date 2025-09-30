import io

import matplotlib.pyplot as plt
from astropy.visualization.wcsaxes.frame import EllipticalFrame
from astropy.wcs import WCS

from mocpy import MOC

def get_crossmatch_plot(obj, moc):
    """
    Returns a PNG image of the skymap with the object overlaid.

    Parameters
    ----------
    obj : dict
        Object with {"id", "ra", "dec"} in degrees.
    moc : MOC
        The MOC object representing the skymap.

    Returns
    -------
    bytes : BytesIO
        A BytesIO object containing the PNG image data.
    """
    wcs = WCS({
        "naxis": 2,
        "naxis1": 1620,
        "naxis2": 810,
        "crpix1": 810.5,
        "crpix2": 405.5,
        "cdelt1": -0.2,
        "cdelt2": 0.2,
        "ctype1": "RA---AIT",
        "ctype2": "DEC--AIT",
        "crval1": 0.0,
        "crval2": 0.0,
    })

    fig = plt.figure(figsize=(10, 5))
    ax = fig.add_subplot(1, 1, 1, projection=wcs, frame_class=EllipticalFrame)
    moc.fill(ax=ax, wcs=wcs, alpha=0.4, color="red")
    moc.border(ax=ax, wcs=wcs, color="red")
    ax.grid()
    ax.coords[0].set_ticklabel_visible(False)
    ax.scatter(obj["ra"], obj["dec"], transform=ax.get_transform("world"),marker='*',
               s=120, c="blue", edgecolor="black", label=obj["id"], zorder=2)

    bytes = io.BytesIO()
    plt.savefig(bytes, format="png", bbox_inches="tight")
    plt.close(fig)
    bytes.seek(0)
    return bytes