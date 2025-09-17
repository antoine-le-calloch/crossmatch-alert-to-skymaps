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

    def api(self, method: str, endpoint: str, data=None, return_raw=False):
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
        return_raw : bool, optional
            If True, return raw response text instead of JSON

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

        if return_raw:
            return response.status_code, response.text

        try:
            body = response.json()
        except Exception:
            raise ValueError(f'Error parsing JSON response: {response.text}')
        
        return response.status_code, body