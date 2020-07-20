"""
Citations Manager module

Implemented as a falcon web application, https://falcon.readthedocs.io/en/stable/


"""
import json
import time
import pytz
import falcon
import logging
import requests

from pytz import timezone
from collections import OrderedDict
from datetime import datetime, timedelta
from urllib.parse import quote_plus, unquote, urlparse


from d1_metrics.metricsdatabase import  MetricsDatabase


DEFAULT_CITATIONS_CONFIGURATION = {
    "solr_query_url": "https://cn.dataone.org/cn/v2/query/solr/"
}

# List of characters that should be escaped in solr query terms
SOLR_RESERVED_CHAR_LIST = [
    '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':'
]


class CitationsManager:
    """
    This class manages the storage and retrieval of citations
    """


    def __init__(self):
        self._config = DEFAULT_CITATIONS_CONFIGURATION
        self.request = {}
        self.response = {}
        self.logger = logging.getLogger('citations_service.' + __name__)


    def on_get(self, req, resp):
        """
        The method assigned to the GET end point

        :param req: HTTP Request object
        :param resp: HTTP Response object
        :return: HTTP Response object
        """
        # taking query parametrs from the HTTP GET request and forming citations_request Object
        self.logger.debug("enter on_get")
        citations_request = {}

        # Setting up the auto expiry time stamp for the caching requests
        current_time = datetime.now()
        tomorrow = current_time + timedelta(1)

        # Setting the GMT offset to get the local time in Pacific
        # Note: Day Light Savings time difference is not set
        midnight = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=7, minute=0, second=0)
        secs = ((midnight - current_time).seconds)

        expiry_time = datetime.now() + timedelta(seconds=secs)

        query_param = urlparse(unquote(req.url))

        if ("=" in query_param.query):
            citations_request = json.loads((query_param.query).split("=", 1)[1])
            resp.body = json.dumps(self.process_request(citations_request), ensure_ascii=False)
        else:
            resp.body = json.dumps(citations_request, ensure_ascii=False)

        # The following line can be omitted because 200 is the default
        # status returned by the framework, but it is included here to
        # illustrate how this may be overridden as needed.
        resp.status = falcon.HTTP_200
        resp.set_headers({"Expires": expiry_time.strftime("%a, %d %b %Y %H:%M:%S GMT")})
        self.logger.debug("exit on_get")


    def on_post(self, req, resp):
        """
        The method assigned to the post end point
        :param req: HTTP Request object
        :param resp: HTTP Response object
        :return: HTTP Response object
        """
        self.logger.debug("enter on_post")
        request_string = req.stream.read().decode('utf8')

        citations_request = json.loads(request_string)

        resp.body = json.dumps(self.process_request(citations_request), ensure_ascii=False)

        # The following line can be omitted because 200 is the default
        # status returned by the framework, but it is included here to
        # illustrate how this may be overridden as needed.
        resp.status = falcon.HTTP_200
        self.logger.debug("exit on_post")


    def process_request(self, citations_request):
        """

        :param citations_request:
        :return:
        """
        if (citations_request["request_type"] == "dataset"):
            return (self.process_dataset_citation_registration(citations_request))

        if (citations_request["request_type"] == "batch"):
            return (self.process_batch_citation_registration(citations_request))

        return {}


    def process_dataset_citation_registration(self, citations_request):
        """

        :param citations_request:
        :return:
        """
        response = {
            "message": "Submission successful"
        }

        if citations_request["metadata"][0]["target_id"] is not None:
            target_id = citations_request["metadata"][0]["target_id"]

        if citations_request["metadata"][0]["target_id"] is not None:
            source_id = citations_request["metadata"][0]["source_id"]

        if citations_request["metadata"][0]["target_id"] is not None:
            relation_type = citations_request["metadata"][0]["relation_type"]

        # retrieve metadata from


        return response
