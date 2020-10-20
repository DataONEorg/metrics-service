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


from d1_metrics_service import pid_resolution
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
        # taking query parametrs from the HTTP GET request and forming metricsRequest Object
        self.logger.debug("enter on_get")
        metrics_request = {}

        query_param = urlparse(unquote(req.url))

        if ("=" in query_param.query):
            metrics_request = json.loads((query_param.query).split("=", 1)[1])
            resp.body = json.dumps(self.process_citation_request(metrics_request), ensure_ascii=False)
        else:
            resp.body = json.dumps(metrics_request, ensure_ascii=False)

        # The following line can be omitted because 200 is the default
        # status returned by the framework, but it is included here to
        # illustrate how this may be overridden as needed.
        resp.status = falcon.HTTP_200
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


    def process_citation_request(self, metrics_request):
        """

        :return: MetricsResponse Object
        """
        t_0 = time.time()
        self.logger.debug("enter process_request. metrics_request=%s", str(metrics_request))
        self.request = metrics_request
        self.response["metricsRequest"] = metrics_request
        filter_by = self.request['filterBy']
        results = {}
        resultDetails = []

        if (len(filter_by) > 0):
            filter_type = filter_by[0]['filterType'].lower()
            interpret_as = filter_by[0]['interpretAs'].lower()
            n_filter_values = len(filter_by[0]['values'])

            self.logger.debug("process_request: filter_type=%s, interpret_as=%s, n_filter_values=%d",
                              filter_type, interpret_as, n_filter_values)

            if filter_type == "dataset" and interpret_as == "list":
                if n_filter_values == 1:
                    resultDetails = self.getDatasetCitations(filter_by[0]["values"])

        self.response["resultDetails"] = resultDetails
        self.logger.debug("exit process_request, duration=%fsec", time.time() - t_0)
        return self.response


    def getDatasetCitations(self, PIDs):
        """

        :param PIDs:
        :return:
        """
        t_start = time.time()
        resultDetails = {}
        resultDetails["citations"] = []

        PIDDict = pid_resolution.getResolvePIDs(PIDs)
        PIDs = PIDDict[PIDs[0]]

        totalCitations, resultDetails["citations"] = self.gatherCitations(PIDs)
        return resultDetails


    def gatherCitations(self, PIDs, metrics_database=None):
        """

        :param PIDs:
        :param metrics_database:
        :return:
        """
        # Retreive the citations if any!
        t_0 = time.time()
        self.logger.debug("enter gatherCitations")
        self.logger.debug("enter gatherCitations")
        if metrics_database is None:
            metrics_database = MetricsDatabase()
            metrics_database.connect()
        csr = metrics_database.getCursor()
        sql = 'SELECT target_id,source_id,source_url,link_publication_date,origin,title,publisher,journal,volume,page,year_of_publishing FROM citations;'

        citations = []
        citationCount = 0
        try:
            csr.execute(sql)
            rows = csr.fetchall()

            for i in rows:
                citationObject = {}
                for j in PIDs:
                    # Special use case for Dryad datasets.
                    if ('?' in j.lower()):
                        j = j.split("?")[0]
                    if i[0].lower() in j.lower():

                        citationCount = citationCount + 1
                        citationObject["target_id"] = i[0]
                        citationObject["source_id"] = i[1]
                        citationObject["source_url"] = i[2]
                        citationObject["link_publication_date"] = i[3]
                        citationObject["origin"] = i[4]
                        citationObject["title"] = i[5]
                        citationObject["publisher"] = i[6]
                        citationObject["journal"] = i[7]
                        citationObject["volume"] = i[8]
                        citationObject["page"] = i[9]
                        citationObject["year_of_publishing"] = i[10]
                        citations.append(citationObject)
                        # We don't want to add duplicate citations for all the objects of the dataset
                        break
        except Exception as e:
            print('Database error!\n{0}', e)
        finally:
            pass
        self.logger.debug("exit gatherCitations, elapsed=%fsec", time.time()-t_0)
        return (citationCount, citations)

