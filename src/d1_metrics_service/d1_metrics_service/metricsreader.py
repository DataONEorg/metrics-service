"""
Metrics Reader module
"""
import json
import falcon
from urllib.parse import urlparse
from urllib.parse import unquote
import requests
from d1_metrics.metricselasticsearch import MetricsElasticSearch
from datetime import datetime


DEFAULT_REPORT_CONFIGURATION={
    "solr_query_url": "https://cn.dataone.org/cn/v2/query/solr/?"
}


class MetricsReader:
    """
    This class parses the metricsRequest obeject
    and based on the filters queries the appropriate
    method of the ApplicationDAO class.
    """
    request = {}
    response = {}


    def __init__(self):
        self._config = DEFAULT_REPORT_CONFIGURATION

    def on_get(self, req, resp):
        """
        The method assigned to the post end point
        :param req: HTTP Request object
        :param resp: HTTP Response object
        :return: HTTP Response object
        """

        #taking query parametrs from the HTTP GET request and forming metricsRequest Object
        metrics_request = {}
        query_param = urlparse(unquote(req.url))



        metrics_request = json.loads((query_param.query).split("=")[1])


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
        metrics_page = self.request['metricsPage']
        filter_by = self.request['filterBy']
        metrics = self.request['metrics']
        group_by = self.request['groupBy']
        results = {}
        if (len(filter_by) > 0):
            if filter_by[0]['filterType'] == "dataset" and filter_by[0]['interpretAs'] == "list":
                results = self.getSummaryMetricsPerDataset(filter_by[0]["values"])
        self.response["results"] = results

        return self.response

    def getSummaryMetricsPerDataset(self, PIDs):
        """
        Queries the Elastic Search and retrieves the summary metrics for a given dataset.
        This information is used to populate the dataset landing pages,
        :param PIDs:
        :return:
        """
        metrics_elastic_search = MetricsElasticSearch(PIDs)
        metrics_elastic_search.connect()
        search_body = [
            {
                "term": {"event.key": "read"}
            },
            {
                "terms": {
                    "pid.key": self.resolvePIDs(PIDs=PIDs)
                }

            }
        ]
        aggregation_body = {
            "pid_list": {
                "composite": {
                    "size": 100,
                    "sources": [
                        {
                            "country": {
                                "terms": {
                                    "field": "geoip.country_code2.keyword"
                                }
                            }
                        },
                        {
                            "format": {
                                "terms": {
                                    "field": "formatType"
                                }
                            }
                        }
                    ]
                }
            }
        }
        pid = self.response["filterBy"][0]["values"]
        self.response["filterBy"][0]["values"] = self.resolvePIDs(PIDs=pid)
        self.request["filterBy"][0]["values"] = self.response["filterBy"][0]["values"]

        start_date = "01/01/2000"
        end_date = datetime.today().strftime('%m/%d/%Y')

        if(len(self.response["filterBy"]) > 1):
            if (self.response["filterBy"][1]["filterType"] == "month" and self.response["filterBy"][1]["interpretAs"] == "range"):
                start_date = self.response["filterBy"][1]["values"][0]
                end_date = self.response["filterBy"][1]["values"][1]

            monthObject = {
                "month": {
                    "date_histogram": {
                        "field": "dateLogged",
                        "interval":"month"
                    }
                }
            }
            aggregation_body["pid_list"]["composite"]["sources"].append(monthObject)
        data = metrics_elastic_search.iterate_composite_aggregations(search_query=search_body,
                                                                     aggregation_query=aggregation_body,
                                                                     start_date=datetime.strptime(start_date,'%m/%d/%Y'),
                                                                     end_date=datetime.strptime(end_date,'%m/%d/%Y'))

        return (self.formatData(data))


    def formatData(self, data):
        """
        Formats the data into the specified Swagger format
        :param data:
        :return:
        """
        return data["aggregations"]


    def resolvePIDs(self, PIDs):
        """
        Checks for the versions and obsolecence chain of the given PID
        :param PID:
        :return: A list of pids for previous versions and their data + metadata objects
        """

        # get the ids for all the previous versions and their data / metadata object till the current `pid` version
        # p.s. this might not be the latest version!
        for i in PIDs:
            queryString = 'q=id:"' + i + '"&fl=documents,obsoletes&wt=json'
            response = requests.get(url=self._config["solr_query_url"], params=queryString).json()
            if(response["response"]["numFound"] > 0):
                # Checks if the pid has any data / metadata objects
                if "documents" in response["response"]["docs"][0]:
                    for j in response["response"]["docs"][0]["documents"]:
                        if j not in PIDs:
                            PIDs.append(j)

                # Checks for the previous versions of the pid
                if "obsoletes" in response["response"]["docs"][0]:
                    if response["response"]["docs"][0]["obsoletes"] not in PIDs:
                        PIDs.append(response["response"]["docs"][0]["obsoletes"])
        # return response.json()
        return PIDs