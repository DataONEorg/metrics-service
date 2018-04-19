"""
GetMetrics Module of the API. Handles the GeET Requests.
"""

import json
import falcon

from .metricshandler import MetricsHandler



class GetMetrics(object):
    """
    This class is used to create a GET Resource
    and map it with the GET endpoint of the API
    """

    def on_get(self, req, resp):
        """
        The method assigned to the post end point
        :param req: HTTP Request object
        :param resp: HTTP Response object
        :return: HTTP Response object
        """

        metrics_request = json.loads(req.get_header('metricsRequest'))

        metrics_handler = MetricsHandler(metrics_request)

        resp.body = json.dumps(metrics_handler.process_request(), ensure_ascii=False)

        # The following line can be omitted because 200 is the default
        # status returned by the framework, but it is included here to
        # illustrate how this may be overridden as needed.
        resp.status = falcon.HTTP_200
