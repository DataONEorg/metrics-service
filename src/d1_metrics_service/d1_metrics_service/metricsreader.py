"""
Metrics Reader module

Implemented as a falcon web application, https://falcon.readthedocs.io/en/stable/


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
import logging
import time

from . import pid_resolution

DEFAULT_REPORT_CONFIGURATION={
    "solr_query_url": "https://cn.dataone.org/cn/v2/query/solr/?"
}


class MetricsReader:
    """
    This class parses the metricsRequest object
    and based on the filters queries the Elastic Search for
    results
    """

    def __init__(self):
        self._config = DEFAULT_REPORT_CONFIGURATION
        self.request = {}
        self.response = {}
        self.logger = logging.getLogger('metrics_service.' + __name__)
        self.req_session = requests.session()


    def on_get(self, req, resp):
        """
        The method assigned to the GET end point

        :param req: HTTP Request object
        :param resp: HTTP Response object
        :return: HTTP Response object
        """
        #taking query parametrs from the HTTP GET request and forming metricsRequest Object
        self.logger.debug("enter on_get")
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

        metrics_request = json.loads(request_string)

        resp.body = json.dumps(self.process_request(metrics_request), ensure_ascii=False)

        # The following line can be omitted because 200 is the default
        # status returned by the framework, but it is included here to
        # illustrate how this may be overridden as needed.
        resp.status = falcon.HTTP_200
        self.logger.debug("exit on_post")



    def process_request(self, metrics_request):
        """
        This method parses the filters of the
        MetricsRequest object
        :return: MetricsResponse Object
        """
        t_0 = time.time()
        self.logger.debug("enter process_request. metrics_request=%s", str(metrics_request))
        self.request = metrics_request
        self.response["metricsRequest"] = metrics_request
        metrics_page = self.request['metricsPage']
        filter_by = self.request['filterBy']
        metrics = self.request['metrics']
        group_by = self.request['groupBy']
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
                    results, resultDetails = self.getSummaryMetricsPerDataset(filter_by[0]["values"])

            if (filter_type == "catalog" or filter_type == "package") and interpret_as == "list":
                if filter_type == "catalog" and n_filter_values == 1:
                    results, resultDetails = self.getSummaryMetricsPerDataset(filter_by[0]["values"])

                if filter_type == "package" and n_filter_values == 1:
                    results, resultDetails = self.getSummaryMetricsPerCatalog(filter_by[0]["values"], filter_type)

                if n_filter_values > 1:
                    #Called when browsing the search UI for example
                    results, resultDetails = self.getSummaryMetricsPerCatalog(filter_by[0]["values"], filter_type)

        self.response["results"] = results
        self.response["resultDetails"] = resultDetails
        self.logger.debug("exit process_request, duration=%fsec", time.time()-t_0)
        return self.response



    def getSummaryMetricsPerDataset(self, PIDs):
        """
        Queries the Elastic Search and retrieves the summary metrics for a given dataset.
        This information is used to populate the dataset landing pages.
        :param PIDs:
        :return: A dictionary containing lists of all the facets specified in the metrics_request
        """
        t_start = time.time()
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        PIDs = self.resolvePIDs(PIDs)
        t_delta = time.time() - t_start
        self.logger.debug('getSummaryMetricsPerDataset:t1=%.4f', t_delta)

        # Getting Obsoletes dictionary
        #manager = multiprocessing.Manager()
        #obsoletes_dict = manager.dict()

        obsoletes_dict = pid_resolution.getObsolescenceChain( PIDs, max_depth=1 )
        #with multiprocessing.Pool() as pool:
        #    for pid in PIDs:
        #      results[pid] = pool.apply_async(self.resolveDataPackagePID, obsoletes_dict, pid)
        #    pool.close()
        #    pool.join()
        #for pid in PIDs:
        #    self.logger.debug("getSummaryMetricsPerDataset #004.5 pid=%s", pid)
        #    self.resolveDataPackagePID(obsoletes_dict, pid)

        t_delta = time.time() - t_start
        self.logger.debug('getSummaryMetricsPerDataset:t2=%.4f', t_delta)

        aggregatedPIDs = {}
        for i in PIDs:
            aggregatedPIDs[i] = {
                "filters": {
                    "filters": {
                        "pid.key": {
                            "term": {
                                "pid.key": i
                            }
                        }
                    }
                }
            }

        search_body = [
            {
                "term": {"event.key": "read"}
            },
            {
                "terms": {
                    "pid.key": PIDs
                }

            },
            {
                "exists": {
                    "field": "geoip.country_code2.keyword"
                }
            },
            {
                "exists": {
                    "field": "sessionId"
                }
            },
            {
                "terms": {
                    "formatType": [
                        "DATA",
                        "METADATA"
                    ]
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
                },
                "aggs" : aggregatedPIDs
            },
            "package_pid_list": {
                "composite": {
                    "sources": [
                      {
                        "format": {
                          "terms": {
                            "field": "formatType"
                          }
                        }
                      }
                    ]
                },
                "aggs": aggregatedPIDs
            }
        }
        pid = self.response["metricsRequest"]["filterBy"][0]["values"]
        self.response["metricsRequest"]["filterBy"][0]["values"] = PIDs
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

        obsoletesDictionary = {k: str(v) for k, v in obsoletes_dict.items()}

        t_delta = time.time() - t_start
        self.logger.debug('getSummaryMetricsPerDataset:t3=%.4f', t_delta)
        return (self.formatDataPerDataset(data, PIDs, obsoletesDictionary))



    def formatDataPerDataset(self, data, PIDs, obsoletesDictionary):
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
            "citations": [],
            "datasets": []

        }
        resultDetails = {}
        resultDetails["citations"] = []
        citationDict = {}

        totalCitations,resultDetails["citations"] = self.gatherCitations(PIDs)
        resultDetails["metrics_package_counts"] = self.parsePackageCounts(data, PIDs, obsoletesDictionary)
        appendedCitations = False

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


        for citationObject in resultDetails["citations"]:
            if(citationObject["link_publication_date"][:7] in citationDict):
                citationDict[citationObject["link_publication_date"][:7]] = citationDict[citationObject["link_publication_date"][:7]] + 1
            else:
                citationDict[citationObject["link_publication_date"][:7]] = 1


        if ("country" in self.response["metricsRequest"]["groupBy"]):
            # Parse the dictionary to form the expected output in the form of lists
            for months in records:
                appendedCitations = False
                for country in records[months]:
                    results["months"].append(months)
                    results["country"].append(country)
                    if("downloads" in self.response["metricsRequest"]["metrics"]):
                        if "downloads" in records[months][country]:
                            results["downloads"].append(records[months][country]["downloads"])
                        else:
                            results["downloads"].append(0)

                    # Views for the given time period.
                    if ("views" in self.response["metricsRequest"]["metrics"]):
                        if "views" in records[months][country]:
                            results["views"].append(records[months][country]["views"])
                        else:
                            results["views"].append(0)

                    if ("citations" in self.response["metricsRequest"]["metrics"]):
                        if(not appendedCitations):
                            citationCount = 0
                            if(months in citationDict):
                                results["citations"].append(citationDict[months])
                            else:
                                results["citations"].append(0)
                            appendedCitations = True

        else:
            for months in records:
                totalDownloads = 0
                totalViews = 0
                results["months"].append(months)
                for country in records[months]:
                    if "downloads" in records[months][country]:
                        totalDownloads = totalDownloads + records[months][country]["downloads"]
                    if "views" in records[months][country]:
                        totalViews = totalViews + records[months][country]["views"]


                if("downloads" in self.response["metricsRequest"]["metrics"]):
                    results["downloads"].append(totalDownloads)

                if ("views" in self.response["metricsRequest"]["metrics"]):
                    results["views"].append(totalViews)

                if ("citations" in self.response["metricsRequest"]["metrics"]):
                    if months in citationDict:
                        results["citations"].append(citationDict[months])
                    else:
                        results["citations"].append(0)

        for months, totals in citationDict.items():
            if months not in results["months"]:
                results["months"].append(months)
                if("country" in self.response["metricsRequest"]["groupBy"]):
                    results["country"].append("US")
                if ("downloads" in self.response["metricsRequest"]["metrics"]):
                    results["downloads"].append(0)

                if ("views" in self.response["metricsRequest"]["metrics"]):
                    results["views"].append(0)

                if ("citations" in self.response["metricsRequest"]["metrics"]):
                    results["citations"].append(totals)

        return results, resultDetails



    def resolvePIDs(self, PIDs, req_session=None):
        """
        Checks for the versions and obsolecence chain of the given PID
        :param PID:
        :return: A list of pids for previous versions and their data + metadata objects
        """
        logger = logging.getLogger('resolvePIDs')
        logger.debug("enter resolvePIDs")
        if req_session is None:
            req_session = requests.Session()

        PIDstring = PIDs[0]

        # get the ids for all the previous versions and their data / metadata object till the current `pid` version
        # p.s. this might not be the latest version!

        # fl = "documents, obsoletes, resourceMap"
        # q = "{!join from=resourceMap to=resourceMap}"

        queryString = 'q={!join from=resourceMap to=resourceMap}id:"' + PIDstring + '"&fl=id&wt=json'

        resp = req_session.get(url=self._config["solr_query_url"], params=queryString)

        if (resp.status_code == 200):
            PIDs = self.parseResponse(resp, PIDs)

        callSolr = True
        # print(PIDs)
        # print(type(PIDs))
        while (callSolr):

            # Querying for all the PIDs that we got from the previous iteration
            # Would be a single PID if this is the first iteration.
            identifier = '(("' + '") OR ("'.join(PIDs) + '"))'

            # Forming the query dictionary to be sent as a file to the Solr endpoint via the HTTP Post request.
            queryDict = {}
            queryDict["fq"] = (None, 'id:* AND resourceMap:' + identifier)
            queryDict["fl"] = (None, 'id,documents,obsoletes,resourceMap')
            queryDict["wt"] = (None, "json")

            # Getting length of the array from previous iteration to control the loop
            prevLength = len(PIDs)

            resp = req_session.post(url=self._config["solr_query_url"], files=queryDict)

            if (resp.status_code == 200):
                PIDs = self.parseResponse(resp, PIDs)

            if (prevLength == len(PIDs)):
                callSolr = False

        logger.debug("resolvePIDs response = %s", json.dumps(PIDs))
        logger.debug("exit resolvePIDs")
        return PIDs



    def parseResponse(self, resp, PIDs):
        response = resp.json()

        for doc in response["response"]["docs"]:
            # Checks if the pid has any data / metadata objects
            if "id" in doc:
                if doc["id"] not in PIDs:
                    PIDs.append(doc["id"])

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

        return PIDs



    def gatherCitations(self, PIDs, metrics_database=None):
        # Retreive the citations if any!
        t_0 = time.time()
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



    def getSummaryMetricsPerCatalog(self, requestPIDArray, a_type):
        """
        Queries the Elastic Search and retrieves the summary metrics for a given DataCatalog pid Array.
        This information is used to populate the DataCatalog and Search pages.
        :param requestPIDArray: Array of PIDs of datasets on DataCatalog page or Search page
        :return:
        """
        t_0 = time.time()
        self.logger.debug("enter getSummaryMetricsPerCatalog")
        catalogPIDs = {}
        combinedPIDs = []
        masterProcess = []
        for i in requestPIDArray:
            if i not in catalogPIDs:
                catalogPIDs[i] = []
                catalogPIDs[i].append(i)

        self.catalogPIDs = catalogPIDs

        return_dict = {}

        self.logger.debug("getSummaryMetricsPerCatalog #004")
        if a_type == "catalog":
          return_dict = pid_resolution.getResolvePIDs(catalogPIDs)
        elif a_type == "package":
          return_dict = pid_resolution.getObsolescenceChain(catalogPIDs)
        #    PIDs = self.resolvePackagePIDs([PID, ], req_session=req_session)
        #return_dict[PID] = PIDs

        #for pid in catalogPIDs:
        #    self.logger.debug("getSummaryMetricsPerCatalog #004.5 pid=%s", pid)
        #    self.resolveCatalogPID(return_dict, a_type, pid, req_session=req_session)
        self.logger.debug("getSummaryMetricsPerCatalog #005: %s", str(return_dict))

        for subProcess in masterProcess:
            subProcess.join()

        for i in catalogPIDs:
            catalogPIDs[i] = return_dict[i]

        for i in catalogPIDs:
            combinedPIDs.extend(catalogPIDs[i])

        aggregatedPIDs = {}
        for i in catalogPIDs:
            aggregatedPIDs[i] = {
                "filters": {
                    "filters": {
                        "pid.key": {
                            "terms": {
                                "pid.key": catalogPIDs[i]
                            }
                        }
                    }
                }
            }

        # Setting the query for the data catalog page
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        search_body = [
            {
                "term": {"event.key": "read"}
            },
            {
                "terms": {
                    "pid.key": combinedPIDs
                }

            },
            {
                "exists": {
                    "field": "sessionId"
                }
            },
            {
                "exists": {
                    "field": "geoip.country_code2.keyword"
                }
            },
            {
                "terms": {
                    "formatType": [
                        "DATA",
                        "METADATA"
                    ]
                }
            }
        ]
        aggregation_body = {
            "pid_list": {
              "composite": {
                "size": 100,
                "sources": [
                  {
                    "format": {
                      "terms": {
                        "field": "formatType"
                      }
                    }
                  }
                ]
              },
              "aggs": aggregatedPIDs
            }
        }

        start_date = "01/01/2000"
        end_date = datetime.today().strftime('%m/%d/%Y')

        data = metrics_elastic_search.iterate_composite_aggregations(search_query=search_body,
                                                                     aggregation_query=aggregation_body,
                                                                     start_date=datetime.strptime(start_date,
                                                                                                  '%m/%d/%Y'),
                                                                     end_date=datetime.strptime(end_date, '%m/%d/%Y'))

        # return {}, {}
        # return data, {}
        self.logger.debug("exit getSummaryMetricsPerCatalog, duration=%fsec", time.time()-t_0)
        return (self.formatDataPerCatalog(data, catalogPIDs))



    def formatDataPerCatalog(self, data, catalogPIDs):
        dataCounts = {}
        metadataCounts = {}
        downloads = []
        views = []
        results = {
            "downloads": [],
            "views": [],
            "citations": [],
            "datasets": [],
            "country": [],
            "months": []
            # "tempDict" : []
        }

        metrics_database = MetricsDatabase()
        metrics_database.connect()
        for i in catalogPIDs:
            count, cits = self.gatherCitations(catalogPIDs[i], metrics_database=metrics_database)
            results["citations"].append(count)

        for i in data["aggregations"]["pid_list"]["buckets"]:
            if i["key"]["format"] == "DATA":
                dataCounts = i
            if i["key"]["format"] == "METADATA":
                metadataCounts = i

        for i in catalogPIDs:
            if i in dataCounts:
                results["downloads"].append(dataCounts[i]["buckets"]["pid.key"]["doc_count"])
            else:
                results["downloads"].append(0)

            if i in metadataCounts:
                results["views"].append(metadataCounts[i]["buckets"]["pid.key"]["doc_count"])
            else:
                results["views"].append(0)

            results["datasets"].append(i)

        return  results, {}



    def resolveCatalogPID(self, return_dict, filter_type, PID, req_session=None):
        '''
        Given identifier, get all versions and obsoleted (catalog) or
        Args:
          return_dict:
          filter_type:
          PID:
          req_session:

        Returns:

        '''

        if req_session is None:
            req_session = requests.Session()
        if filter_type == "catalog":
            PIDs = self.resolvePIDs([PID, ], req_session=req_session)
        if filter_type == "package":
            PIDs = self.resolvePackagePIDs([PID, ], req_session=req_session)
        return_dict[PID] = PIDs
        return [PID, PIDs]



    def resolvePackagePIDs(self, PIDs, req_session=None):
        """
        Checks for the versions and obsolecence chain of the given PID
        :param PID:
        :return: A list of pids for previous versions and their data + metadata objects
        """
        logger = logging.getLogger('resolvePackagePIDs')
        logger.debug("enter resolvePackagePIDs")

        callSolr = True
        if req_session is None:
            req_session = requests.Session()
        while (callSolr):

            # Querying for all the PIDs that we got from the previous iteration
            # Would be a single PID if this is the first iteration.
            identifier = '(("' + '") OR ("'.join(PIDs) + '"))'

            # Forming the query dictionary to be sent as a file to the Solr endpoint via the HTTP Post request.
            queryDict = {}
            queryDict["fq"] = (None, 'id:*' + identifier)
            queryDict["fl"] = (None, 'obsoletes')
            queryDict["wt"] = (None, "json")

            # Getting length of the array from previous iteration to control the loop
            prevLength = len(PIDs)

            resp = req_session.post(url=self._config["solr_query_url"], files=queryDict)

            if (resp.status_code == 200):
                PIDs = self.parseResponse(resp, PIDs)

            if (prevLength == len(PIDs)):
                callSolr = False

        logger.debug("exit resolvePackagePIDs")
        return PIDs



    def resolveDataPackagePID(self, obsoletes_dict, PID, req_session=None):

        if req_session is None:
            req_session = requests.Session()
        # Forming the query dictionary to be sent as a file to the Solr endpoint via the HTTP Post request.
        queryString = 'q=id:"' + PID + '"&fl=obsoletes&wt=json'
        params = {'q': 'id:"' + PID + '"',
                  'fl': 'obsoletes',
                  'wt': 'json'}

        res = None
        resp = req_session.get(url=self._config["solr_query_url"], params=params)
        if (resp.status_code == 200):
            PIDs = self.parseResponse(resp, [])
            if (len(PIDs) != 0):
                obsoletes_dict[PID] = PIDs[0]
                res = PIDs[0]
            else:
                pass
        return res



    def parsePackageCounts(self, data, PIDs, obsoletesDictionary):
        resultDict = {}
        pid_list = []
        downloads = {}
        views = {}


        for i in data["aggregations"]["package_pid_list"]["buckets"]:
            if(i["key"]["format"] == "DATA"):
                downloads = i
            if(i["key"]["format"] == "METADATA"):
                views = i

        for i in obsoletesDictionary:
            if obsoletesDictionary[i] in PIDs:
                PIDs.remove(obsoletesDictionary[i])

        for i in PIDs:
            target = i
            if target in views:
                viewCount = views[target]["buckets"]["pid.key"]["doc_count"]
            else:
                viewCount = 0
            if target in downloads:
                downloadCount = downloads[target]["buckets"]["pid.key"]["doc_count"]
            else:
                downloadCount = 0
            while(target in obsoletesDictionary):
                target = obsoletesDictionary[target]
                if target in views:
                    viewCount += views[target]["buckets"]["pid.key"]["doc_count"]
                if target in downloads:
                    downloadCount = downloads[target]["buckets"]["pid.key"]["doc_count"]
            resultDict[i] = {}
            resultDict[i]["viewCount"] = viewCount
            resultDict[i]["downloadCount"] = downloadCount

        return resultDict



if __name__ == "__main__":
    mr = MetricsReader()
