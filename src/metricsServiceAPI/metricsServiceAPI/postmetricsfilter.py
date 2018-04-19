"""
PostMetricsFilters module of the PAI. Handles the POST request.
"""

import json
import falcon

from .metricshandler import MetricsHandler


class PostMetricsFilters:
    """
    This class is used to create a POST Resource
    and map it with the POST endpoint of the API
    """

    def on_post(self, req, resp):
        """
        The method assigned to the post end point
        :param req: HTTP Request object
        :param resp: HTTP Response object
        :return: HTTP Response object
        """
        metrics_request = json.loads(req.stream.read())

        metrics_handler = MetricsHandler(metrics_request)

        resp.body = json.dumps(metrics_handler.process_request(), ensure_ascii=False)

        # The following line can be omitted because 200 is the default
        # status returned by the framework, but it is included here to
        # illustrate how this may be overridden as needed.
        resp.status = falcon.HTTP_200
