"""
Metrics Reader module
"""
import json
import falcon
from urllib.parse import urlparse

from d1_metrics.metricsdatabase import MetricsDatabase


class MetricsReader:
    """
    This class parses the metricsRequest obeject
    and based on the filters queries the appropriate
    method of the ApplicationDAO class.
    """
    request = {}
    response = {}

    def on_get(self, req, resp):
        """
        The method assigned to the post end point
        :param req: HTTP Request object
        :param resp: HTTP Response object
        :return: HTTP Response object
        """

        #taking query parametrs from the HTTP GET request and forming metricsRequest Object
        metrics_request = {}
        query_param = urlparse(req.url)

        for i in query_param.query.split("&"):
            query = i.split(":")
            metrics_request[query[0]] = ":".join(query[1:])


        resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False)

        # The following line can be omitted because 200 is the default
        # status returned by the framework, but it is included here to
        # illustrate how this may be overridden as needed.
        resp.status = falcon.HTTP_200

    def on_post(self, req, resp):
        """
        The method assigned to the post end point
        :param req: HTTP Request object
        :param resp: HTTP Response object
        :return: HTTP Response object
        """
        metrics_request = json.loads(req.stream.read())

        resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False)

        # The following line can be omitted because 200 is the default
        # status returned by the framework, but it is included here to
        # illustrate how this may be overridden as needed.
        resp.status = falcon.HTTP_200



    def process_request(self, metrics_request):
        """
        This method parses the filters of the
        MetricsRequest object
        :return:
        """
        self.request = metrics_request
        self.response = metrics_request
        metricsdb = MetricsDatabase()
        metrics_page = self.request['metricsPage']
        filter_by = self.request['filterBy']
        metrics = self.request['metrics']
        group_by = self.request['groupBy']

        for items in filter_by:
            if items['filterType'] == "dataset" and items['interpretAs'] == "list":
                results = metricsdb.getSummaryMetricsPerDataset(items['values'])
                self.response.update(results)

        return self.response
