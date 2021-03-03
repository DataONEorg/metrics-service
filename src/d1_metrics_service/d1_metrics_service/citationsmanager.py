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
    "solr_query_url": "https://cn-secondary.dataone.org/cn/v2/query/solr/"
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

        response = self.process_request(citations_request)
        resp.body = json.dumps(response, ensure_ascii=False)

        # The following line can be omitted because 200 is the default
        # status returned by the framework, but it is included here to
        # illustrate how this may be overridden as needed.
        if (response["status_code"] == "202"):
            resp.status = falcon.HTTP_202

        if (response["status_code"] == "500"):
            resp.status = falcon.HTTP_500

        if (response["status_code"] == "400"):
            resp.status = falcon.HTTP_400

        self.logger.debug("exit on_post")


    def process_request(self, citations_request):
        """

        :param citations_request:
        :return:
        """
        response = {
            "message": "Cannot process this type of request",
            "status_code": "500"
        }

        if (citations_request["request_type"] == "dataset"):
            return self.queue_citation_object(citations_request)


        if (citations_request["request_type"] == "batch"):
            return self.queue_citation_object(citations_request)

        return response


    def queue_citation_object(self, citations_request):
        """

        :param citations_request:
        :return:
        """
        response = {
            "message": "",
            "status_code": ""
        }

        try:
            metrics_database = MetricsDatabase()
            metrics_database.queueCitationRequest(citations_request)
            response["message"] = "Successful"
            response["status_code"] = "202"
        except Exception as e:
            response["message"] = e
            response["status_code"] = "500"
        finally:
            pass

        return response

