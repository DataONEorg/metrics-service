"""
Citations Manager module

Implemented as a falcon web application, https://falcon.readthedocs.io/en/stable/


"""
import re
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


from d1_metrics.metricsdatabase import MetricsDatabase
from d1_metrics_service import pid_resolution


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
            self.request = metrics_request
            resp.body = json.dumps(self.process_citation_request(metrics_request), ensure_ascii=False)
        else:
            self.request = metrics_request
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
        self.request = citations_request

        response = self.handle_citation_post_request(citations_request)
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


    def handle_citation_post_request(self, citation_request):
        """
        Takes the citation request and handles the storage job
        :param citation_request:
        :return:
        """
        if citation_request is not None and isinstance(citation_request, dict):
            if "submitter" in citation_request and "citations" in citation_request:
                # handling updated version
                if len(citation_request["citations"]) == 1:
                    return self.register_citation(citation_object=citation_request["citations"][0], submitter=citation_request["submitter"])

                elif len(citation_request["citations"]) > 1:
                    return self.batch_register_citation(citation_request)

            elif "metadata" in citation_request:
                # handling version 1
                return process_request(citation_request)

        response = {
            "message": "Cannot process this type of request",
            "status_code": "500"
        }

        return response


    def register_citation(self, citation_object, submitter):
        """
        Validates citation metadata and registers it into the citation database
        :return:
        """
        invalid_metadata = False
        citations = []
        if citation_object is not None and isinstance(citation_object, dict):

            # validate the required fields
            if ("source_id" in citation_object and "source_id" is not None) and \
                ("related_identifiers" in citation_object and len(citation_object["related_identifiers"]) > 0):

                for related_id_object in citation_object["related_identifiers"]:

                    if "identifier" not in related_id_object or \
                            len(related_id_object["identifier"]) < 1:
                        print("1")
                        invalid_metadata=True

                    if "relation_type" not in related_id_object or \
                            len(related_id_object["relation_type"]) < 1:
                        print("2")
                        invalid_metadata=True

                    if invalid_metadata :

                        response = {
                            "message": "Incomplete Metadata. source_id, and related_identifiers are both required fields",
                            "status_code": "500"
                        }

                        return response

                    identifier  = related_id_object["identifier"]
                    relation_type = related_id_object["relation_type"]
                    source_id = citation_object["source_id"]

                    try:
                        metrics_database = MetricsDatabase()
                        metrics_database.connect()
                        doi_pattern = "^\s*(http:\/\/|https:\/\/)?(doi.org\/|dx.doi.org\/)?(doi: ?|DOI: ?)?(10\.\d{4,}(\.\d)*)\/(\w+).*$"
                        doi_metadata = {}
                        if (re.match(doi_pattern, source_id)):
                            source_doi_index = source_id.index("10.")
                            source_doi = source_id[source_doi_index:]
                            doi_metadata = metrics_database.getDOIMetadata(doi=source_doi)

                        if (re.match(doi_pattern, identifier)):
                            identifier_index = identifier.index("10.")
                            identifier_doi = identifier[identifier_index:]

                        citation_db_object = {}
                        citation_db_object["source_id"] = source_doi
                        citation_db_object["target_id"] = identifier_doi
                        citation_db_object["relation_type"] = relation_type

                        if "source_url" in citation_object:
                            citation_db_object["source_url"] = citation_object["source_url"]
                        elif "source_url" in doi_metadata:
                            citation_db_object["source_url"] = doi_metadata["source_url"]

                        if "link_publication_date" in citation_object:
                            citation_db_object["link_publication_date"] = citation_object["link_publication_date"]
                        elif "link_publication_date" in doi_metadata:
                            citation_db_object["link_publication_date"] = doi_metadata["link_publication_date"]

                        if "origin" in citation_object:
                            citation_db_object["origin"] = citation_object["origin"]
                        elif "origin" in doi_metadata:
                            citation_db_object["origin"] = doi_metadata["origin"]

                        if "title" in citation_object:
                            citation_db_object["title"] = citation_object["title"]
                        elif "title" in doi_metadata:
                            citation_db_object["title"] = doi_metadata["title"]

                        if "publisher" in citation_object:
                            citation_db_object["publisher"] = citation_object["publisher"]
                        elif "publisher" in doi_metadata:
                            citation_db_object["publisher"] = doi_metadata["publisher"]

                        if "journal" in citation_object:
                            citation_db_object["journal"] = citation_object["journal"]
                        elif "journal" in doi_metadata:
                            citation_db_object["journal"] = doi_metadata["journal"]

                        if "volume" in citation_object:
                            citation_db_object["volume"] = citation_object["volume"]
                        elif "volume" in doi_metadata:
                            citation_db_object["volume"] = doi_metadata["volume"]

                        if "page" in citation_object:
                            citation_db_object["page"] = citation_object["page"]
                        elif "page" in doi_metadata:
                            citation_db_object["page"] = doi_metadata["page"]

                        if "year_of_publishing" in citation_object:
                            citation_db_object["year_of_publishing"] = citation_object["year_of_publishing"]
                        elif "year_of_publishing" in doi_metadata:
                            citation_db_object["year_of_publishing"] = doi_metadata["year_of_publishing"]

                        citations.append(citation_db_object)
                        metrics_database.insertCitationObjects(citations_data=citations)
                        response = {
                            "message": "Registered",
                            "status_code": "202"
                        }
                        return response

                    except Exception as e:
                        self.logger.error(e)
                        return self.queue_citation_object(self.request)

        response = {
            "message": "Cannot process this type of request",
            "status_code": "500"
        }

        return response


    def batch_register_citation(self, citation_request):
        """
        Handles registration of multiple citaiton requests
        :return:
        """
        pass


    def queue_citation_object(self, citations_request):
        """
        Queues the citation request
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
        Handles retrieval of Citation object from the database
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
        For a given dataset PID, it resolves the identifiers and checks for a citation hit
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
        Checks for existing citation in the database
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
        sql = 'SELECT target_id,source_id,source_url,link_publication_date,origin,title,publisher,journal,volume,page,year_of_publishing,relation_type FROM citations_test;'

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

                        # form the related identifier object
                        related_identifiers_list = []
                        related_identifier_object = {}
                        related_identifier_object["identifier"] = i[0]
                        related_identifier_object["relation_type"] = "cites" if i[11] is None else i[11]
                        related_identifiers_list.append(related_identifier_object)

                        citationObject["related_identifiers"] = related_identifiers_list

                        citations.append(citationObject)
                        # We don't want to add duplicate citations for all the objects of the dataset
                        break
        except Exception as e:
            print('Database error!\n{0}', e)
        finally:
            pass
        self.logger.debug("exit gatherCitations, elapsed=%fsec", time.time()-t_0)
        return (citationCount, citations)

