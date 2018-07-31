"""
Metrics Service handler module
"""

import falcon

class ServiceHandler:
    """
    This class parses the service Request object and manages the automation service accordingly.
    """

    def __init__(self):
        pass


    def on_get(self, req, resp):
        query_param = urlparse(unquote(req.url))

