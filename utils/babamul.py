import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.api import APIError, REQUEST_TIMEOUT

SKYMAP_SEARCH_TIMEOUT = 60  # seconds, BOOM parses the skymap and ranks its voxels for each search
SKYMAP_SEARCH_LIMIT = 10000  # maximum number of alerts BOOM returns for a skymap search


class Babamul:
    """
    Babamul API client

    Parameters
    ----------
    url : str
        Base URL of the Babamul API (e.g. https://babamul.caltech.edu/api)
    token : str
        Babamul API token
    """

    def __init__(self, url, token):
        self.base_url = url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {token}"
        adapter = HTTPAdapter(max_retries=Retry(
            total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504], raise_on_status=False
        ))
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

    def api(self, method, endpoint, timeout=REQUEST_TIMEOUT, **kwargs):
        """
        Make an API request to Babamul

        Returns
        -------
        dict
            The JSON body of the response, with its message and data
        """
        try:
            response = self.session.request(method, f"{self.base_url}/{endpoint.strip('/')}", timeout=timeout, **kwargs)
        except requests.exceptions.RequestException as e:
            raise APIError(f"Babamul request error in {endpoint} - {type(e).__name__}")
        try:
            body = response.json()
        except ValueError:
            raise APIError(f"Babamul HTTP {response.status_code} {response.reason} in {endpoint}")
        if response.status_code != 200:
            raise APIError(f"Babamul error in {endpoint} - {body.get('message', response.reason)}")
        return body

    def skymap_search(self, survey, skymap_fits_base64, credible_level, start_jd, end_jd):
        """
        Search the alerts of a time window inside the credible region of a skymap

        For a 3D GW skymap, BOOM ranks the voxels (pixel x distance bin) of the skymap by probability
        per unit volume and places each alert at the redshifts of its host galaxy candidates.

        Parameters
        ----------
        survey : str
            Survey of the alerts (e.g. ZTF, LSST)
        skymap_fits_base64 : str
            The FITS file of the skymap, base64-encoded
        credible_level : float
            Credible level of the region (of the volume, for a 3D skymap)
        start_jd, end_jd : float
            Time window of the alerts, in Julian Date

        Returns
        -------
        tuple
            The alerts found, each with host_searched_prob_vol: the 3D credible level of its best host
            (lower is better), or None for an alert without any host redshift, only tested in 2D.
            And whether the search may have missed some alerts of the region.
        """
        body = self.api(
            "POST",
            f"/babamul/surveys/{survey.lower()}/alerts/skymap-search",
            timeout=SKYMAP_SEARCH_TIMEOUT,
            json={
                "skymap_fits_base64": skymap_fits_base64,
                "credible_level": credible_level,
                "start_jd": start_jd,
                "end_jd": end_jd,
                "limit": SKYMAP_SEARCH_LIMIT,
            },
        )
        alerts = body.get("data") or []
        # BOOM only reports in its message that it capped the candidates before the 3D test
        incomplete = len(alerts) >= SKYMAP_SEARCH_LIMIT or "incomplete" in body.get("message", "")
        return alerts, incomplete

    def get_alerts(self, survey, object_id, start_jd, end_jd):
        """
        Get the alerts of an object in a time window

        Returns
        -------
        list
            Alerts, with their candid
        """
        body = self.api(
            "GET",
            f"/babamul/surveys/{survey.lower()}/alerts",
            params={"object_id": object_id, "start_jd": start_jd, "end_jd": end_jd},
        )
        return body.get("data") or []
