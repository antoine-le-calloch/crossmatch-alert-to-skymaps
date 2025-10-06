import io
import time

import requests

class SkyPortal:
    """
    SkyPortal API client

    Parameters
    ----------
    protocol : str
        Protocol to use (http or https)
    host : str
        Hostname of the SkyPortal instance
    port : int
        Port to use
    token : str
        SkyPortal API token
    validate : bool, optional
        If True, validate the SkyPortal instance and token
    
    Attributes
    ----------
    base_url : str
        Base URL of the SkyPortal instance
    headers : dict
        Authorization headers to use
    """

    def __init__(self, instance, token, port=443, validate=True):
        # build the base URL from the protocol, host, and port
        self.base_url = f'{instance}'
        if port not in ['None', '', 80, 443]:
            self.base_url += f':{port}'
        
        self.headers = {'Authorization': f'token {token}'}

        # ping it to make sure it's up, if validate is True
        if validate:
            if not self._ping(self.base_url):
                raise ValueError('SkyPortal API not available')
            
            if not self._auth(self.base_url, self.headers):
                raise ValueError('SkyPortal API authentication failed. Token may be invalid.')
            
    def _ping(self, base_url):
        """
        Check if the SkyPortal API is available
        
        Parameters
        ----------
        base_url : str
            Base URL of the SkyPortal instance
            
        Returns
        -------
        bool
            True if the API is available, False otherwise
        """
        response = requests.get(f"{base_url}/api/sysinfo")
        return response.status_code == 200
    
    def _auth(self, base_url, headers):
        """
        Check if the SkyPortal Token provided is valid

        Parameters
        ----------
        base_url : str
            Base URL of the SkyPortal instance
        headers : dict
            Authorization headers to use

        Returns
        -------
        bool
            True if the token is valid, False otherwise
        """
        response = requests.get(
            f"{base_url}/api/config",
            headers=headers
        )
        return response.status_code == 200

    def ping(self):
        """
        Ping the SkyPortal API to check if it's available

        Returns
        -------
        bool
            True if the API is available, False otherwise
        """
        return self._ping(self.base_url)

    def fetch_all_pages(self, endpoint, payload, item_key):
        """
        Fetch all pages of a paginated API endpoint

        Returns
        -------
        list
            All items from all pages
        """
        items = []
        payload["pageNumber"] = 1
        payload["numPerPage"] = 1000,
        while True:
            results = self.api("GET", endpoint, data=payload)
            items += results[item_key]
            if results["totalMatches"] <= len(items):
                break
            payload["pageNumber"] += 1
            time.sleep(0.3)
        return items

    def api(self, method: str, endpoint: str, data=None, return_response=False):
        """
        Make an API request to SkyPortal

        Parameters
        ----------
        method : str
            HTTP method to use (GET, POST, PUT, PATCH, DELETE)
        endpoint : str
            API endpoint to query
        data : dict, optional
            JSON data to send with the request, as parameters or payload
        return_response : bool, optional
            If True, return the raw response instead of parsing JSON

        Returns
        -------
        int
            HTTP status code
        dict
            JSON response
        """
        endpoint = f'{self.base_url}/{endpoint.strip("/")}'
        if method == 'GET':
            response = requests.request(method, endpoint, params=data, headers=self.headers)
        else:
            response = requests.request(method, endpoint, json=data, headers=self.headers)

        if return_response:
            return response

        try:
            body = response.json()
        except Exception:
            raise ValueError(f'Error parsing JSON response: {response.text}')

        if response.status_code != 200:
            raise ValueError(f'Error in API request: {body}')

        return body.get('data')

    def get_gcn_events(self, dateobs):
        """
        Get GCN events from SkyPortal filtered by dateobs and
        specific tags:
        - GW (any size)
        - NSBH (any size)
        - Fermi < 1000 sq. deg.
        - SVOM (any notice)

        Parameters
        ----------
        dateobs : datetime.datetime
            Date of observation to filter GCN events from

        Returns
        -------
        int
            HTTP status code
        dict
            JSON response
        """
        payload = {
            "startDate": dateobs,
            "excludeNoticeContent": True,
        }

        # Get GCN events with GW, BNS, NSBH or SVOM and without BBH, MLy or Terrestrial tags.
        gcn_events = self.fetch_all_pages(
            "/api/gcn_event",
            {
                **payload,
                "gcnTagKeep":"GW,BNS,NSBH,SVOM",
                "gcnTagRemove": "BBH,MLy,Terrestrial"
            },
            "events"
        )

        # Get GCN events with Fermi tag and localization < 1000 sq.deg.
        gcn_events += self.fetch_all_pages(
            "/api/gcn_event",
            {**payload,"gcnTagKeep": "Fermi","localizationTagKeep": "< 1000 sq.deg"},
            "events"
        )
        return gcn_events

    def download_localization(self, dateobs, localization_name):
        """
        Download localization as a FITS file from SkyPortal.

        Returns
        -------
        int
            HTTP status code
        dict
            JSON response
        """
        response = self.api(
            "GET",
            f"/api/localization/{dateobs}/name/{localization_name}/download",
            return_response=True
        )
        if response.status_code != 200:
            raise ValueError(f"Error fetching localization: {response.text}")
        return io.BytesIO(response.content) # return a BytesIO object containing the FITS file

    def get_objects(self, payload):
        """
        Get objects from SkyPortal

        Parameters
        ----------
        payload : dict
            Dictionary of parameters to send with the request

        Returns
        -------
        int
            HTTP status code
        dict
            JSON response
        """
        return self.fetch_all_pages("/api/candidates", payload, "candidates")