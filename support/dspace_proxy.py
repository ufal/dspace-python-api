import const
from support.dspace_interface.client import DSpaceClient
from support.logs import log
from support.logs import Severity

ERROR = Severity.ERROR


class DspaceRESTProxy:
    """
    Serves as proxy to Dspace REST API.
    Mostly uses attribute d which represents (slightly modified) dspace_client from
    original python rest api by dspace developers
    """

    def __init__(self):
        self.response = None
        self.d = DSpaceClient(api_endpoint=const.API_URL, username=const.user, password=const.password)
        authenticated = self.d.authenticate()
        if not authenticated:
            log(f'Error logging in to dspace REST API at ' + const.API_URL + '! Exiting!', ERROR)
            raise ConnectionError("Cannot connect to dspace!")
        log("Successfully logged in to dspace on " + const.API_URL)

    def get(self, command, params=None, data=None):
        """
        Simple GET of url.
        param command what to append to host.xx/server/api/
        """
        url = const.API_URL + command
        self.response = self.d.api_get(url, params, data)
        return self.response


rest_proxy = DspaceRESTProxy()
