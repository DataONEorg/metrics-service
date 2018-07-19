"""
Metrics Reader module
"""
import json
import falcon
from urllib.parse import urlparse
from urllib.parse import unquote
from urllib.parse import quote_plus
import requests
from d1_metrics.metricselasticsearch import MetricsElasticSearch
from d1_metrics.metricsdatabase import MetricsDatabase
from datetime import datetime


DEFAULT_REPORT_CONFIGURATION={
    "solr_query_url": "https://cn.dataone.org/cn/v2/query/solr/?"
}


class MetricsReader:
    """
    This class parses the metricsRequest obeject
    and based on the filters queries the Elastic Search for
    results
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

        if ("=" in query_param.query):
            metrics_request = json.loads((query_param.query).split("=", 1)[1])
            resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False)
        else:
            resp.body = json.dumps(metrics_request, ensure_ascii=False)

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
        request_string = req.stream.read().decode('utf8')

        metrics_request = json.loads(request_string)

        resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False)

        # The following line can be omitted because 200 is the default
        # status returned by the framework, but it is included here to
        # illustrate how this may be overridden as needed.
        resp.status = falcon.HTTP_200



    def process_request(self, metrics_request):
        """
        This method parses the filters of the
        MetricsRequest object
        :return: MetricsResponse Object
        """
        self.request = metrics_request
        self.response["metricsRequest"] = metrics_request
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
        This information is used to populate the dataset landing pages.
        :param PIDs:
        :return: A dictionary containing lists of all the facets specified in the metrics_request
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
        pid = self.response["metricsRequest"]["filterBy"][0]["values"]
        self.response["metricsRequest"]["filterBy"][0]["values"] = self.resolvePIDs(PIDs=pid)
        self.request["filterBy"][0]["values"] = self.response["metricsRequest"]["filterBy"][0]["values"]

        start_date = "01/01/2000"
        end_date = datetime.today().strftime('%m/%d/%Y')

        if(len(self.response["metricsRequest"]["filterBy"]) > 1):
            if (self.response["metricsRequest"]["filterBy"][1]["filterType"] == "month" and self.response["metricsRequest"]["filterBy"][1]["interpretAs"] == "range"):
                start_date = self.response["metricsRequest"]["filterBy"][1]["values"][0]
                end_date = self.response["metricsRequest"]["filterBy"][1]["values"][1]

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

        return (self.formatData(data, PIDs))


    def formatData(self, data, PIDs):
        """
        Formats the data into the specified Swagger format
        :param data: Dictionary retrieved from the ES
        :param PIDs: List of pids
        :return: A dictionary containing lists of all the facets specified in the metrics_request
        """
        records = {}
        results = {
            "months": [],
            "country": [],
            "downloads": [],
            "views": [],
            "citations": []

        }

        # Retreive the citations if any!
        metrics_database = MetricsDatabase()
        metrics_database.connect()
        csr = metrics_database.getCursor()
        sql = 'SELECT TARGET_ID FROM citations;'
        target_ids = []
        try:
            csr.execute(sql)
            rows = csr.fetchall()
            for i in rows:
                for j in PIDs:
                    # print(i[0])
                    # print(j)
                    if i[0].lower() in j.lower():
                        print("Yes")
                        target_ids.append(i[0])
        except Exception as e:
            print('Database error!\n{0}', e)
        finally:
            pass


        # Combine metrics into a single dictionary
        for i in data["aggregations"]["pid_list"]["buckets"]:
            month = datetime.utcfromtimestamp((i["key"]["month"]//1000)).strftime(('%Y-%m'))
            if month not in records:
                records[month] = {}
            if i["key"]["country"] not in records[month]:
                records[month][i["key"]["country"]] = {}
            if (i["key"]["format"] == "DATA"):
                records[month][i["key"]["country"]]["downloads"] = i["doc_count"]
            if (i["key"]["format"] == "METADATA"):
                records[month][i["key"]["country"]]["views"] = i["doc_count"]
            pass

        # adding citation metric
        citationMonth = datetime.now().strftime('%Y-%m')
        # records[citationMonth]["USA"]["citations"] = len(target_ids)
        if citationMonth in records:
            if "USA" in records[citationMonth]:
                # Assigining the citation to each of the country for now.
                # TODO: Discuss with the team about this.
                records[citationMonth]["USA"]["citations"] = len(target_ids)
        else:
            records[citationMonth] = {
                "USA": {
                    "citations": len(target_ids)
                }
            }

        # Parse the dictionary to form the expected output in the form of lists
        for months in records:
            for country in records[months]:
                results["months"].append(months)
                results["country"].append(country)
                if "downloads" in records[months][country]:
                    results["downloads"].append(records[months][country]["downloads"])
                else:
                    results["downloads"].append(0)

                # Views for the given time period.
                if "views" in records[months][country]:
                    results["views"].append(records[months][country]["views"])
                else:
                    results["views"].append(0)

                if "citations" in records[months][country]:
                    results["citations"].append(records[months][country]["citations"])
                else:
                    results["citations"].append(0)

        #If no log entry found in the ES
        if((len(results["months"])) == 0):
            results["months"] = 0
            results["country"] = 0
            results["downloads"] = 0
            results["views"] = 0
            results["citations"] = 0


        return results


    def resolvePIDs(self, PIDs):
        """
        Checks for the versions and obsolecence chain of the given PID
        :param PID:
        :return: A list of pids for previous versions and their data + metadata objects
        """

        # get the ids for all the previous versions and their data / metadata object till the current `pid` version
        # p.s. this might not be the latest version!

        callSolr = True
        while(callSolr):
            # Querying for all the PIDs that we got from the previous iteration
            # Would be a single PID if this is the first iteration.
            identifier = '(("' + '") OR ("'.join(PIDs) + '"))'

            # Forming the query string and url encoding the identifier to escape special chartacters
            queryString = 'fq=id:' + quote_plus(identifier) + '&fl=documents,obsoletes,resourceMap&wt=json'

            # Querying SOLR
            response = requests.get(url=self._config["solr_query_url"], params=queryString).json()

            for doc in response["response"]["docs"]:
                # Checks if the pid has any data / metadata objects
                if "documents" in doc:
                    for j in doc["documents"]:
                        if j not in PIDs:
                            PIDs.append(j)

                # Checks for the previous versions of the pid
                if "obsoletes" in doc:
                    if doc["obsoletes"] not in PIDs:
                        PIDs.append(doc["obsoletes"])

                # Checks for the resource maps of the pid
                if "resourceMap" in doc:
                    for j in doc["resourceMap"]:
                        if j not in PIDs:
                            PIDs.append(j)
            if(prevLength == len(PIDs)):
                callSolr = False


        print(PIDs)

        return PIDs


if __name__ == "__main__":
    mr = MetricsReader()
    mr.resolvePIDs(["urn:node:KNB.14618012"])