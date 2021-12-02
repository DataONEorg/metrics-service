"""
Metrics Reader module

Implemented as a falcon web application, https://falcon.readthedocs.io/en/stable/


"""
import json
import logging
import time
from collections import OrderedDict
from datetime import datetime, timedelta
from pytz import timezone
import pytz
from urllib.parse import quote_plus, unquote, urlparse

import requests

import falcon
from d1_metrics.metricsdatabase import MetricsDatabase
from d1_metrics.metricselasticsearch import MetricsElasticSearch
from d1_metrics_service import pid_resolution

DEFAULT_REPORT_CONFIGURATION={
    "solr_query_url": "https://cn-secondary.dataone.org/cn/v2/query/solr/"
}

# List of characters that should be escaped in solr query terms
SOLR_RESERVED_CHAR_LIST = [
  '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':'
  ]

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
            metrics_request = json.loads((query_param.query).split("=", 1)[1])
            metrics_response = self.process_request(metrics_request)
            resp.body = json.dumps(metrics_response, ensure_ascii=False)
        else:
            resp.body = json.dumps(metrics_request, ensure_ascii=False)

        resp.status = falcon.HTTP_200
        if "status_code" in metrics_response["resultDetails"]:
            resp.status = metrics_response["resultDetails"]["status_code"]

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

        metrics_request = json.loads(request_string)
        metrics_response = self.process_request(metrics_request)
        resp.body = json.dumps(metrics_response, ensure_ascii=False)

        resp.status = falcon.HTTP_200
        if "status_code" in metrics_response["resultDetails"]:
            resp.status = metrics_response["resultDetails"]["status_code"]

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
                if n_filter_values > 1:
                    #Called when browsing the search UI for example
                    results, resultDetails = self.getSummaryMetricsPerCatalog(filter_by[0]["values"], filter_type)

            if (filter_type == "repository") and interpret_as == "list":
                results, resultDetails = self.getMetricsPerRepository(filter_by[0]["values"][0])

            if (filter_type == "user") and interpret_as == "list":
                # Called when generating metrics for a specific user
                results, resultDetails = self.getMetricsPerUser(filter_by[0]["values"])

            if (filter_type == "group") and interpret_as == "list":
                # Called when generating metrics for a specific user
                results, resultDetails = self.getMetricsPerGroup(filter_by[0]["values"])

            if (filter_type == "portal") and interpret_as == "list":
                collectionQueryFilterObject = {}
                collectionQueryFilterObjectList = list(filter(lambda filter_object: filter_object['filterType'] == 'query', filter_by))
                if ( len(collectionQueryFilterObjectList) ):
                    collectionQueryFilterObject = collectionQueryFilterObjectList[0]
                # Called when generating metrics for a specific portal
                results, resultDetails = self.getMetricsPerPortal(filter_by[0]["values"][0], collectionQueryFilterObject)

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
        PIDDict = pid_resolution.getResolvePIDs(PIDs)
        PIDs = PIDDict[PIDs[0]]
        t_delta = time.time() - t_start
        self.logger.debug('getSummaryMetricsPerDataset:t1=%.4f', t_delta)

        obsoletes_dict = pid_resolution.getObsolescenceChain( PIDs, max_depth=1 )

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
                },
                "aggs": {
                    "unique_doc_count": {
                        "cardinality": {
                            "field": "eventId"
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
                                    "field": "geoip.country_code2.keyword",
                                    "missing_bucket": "true"
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
                "aggs": {
                    "unique_doc_count": {
                        "cardinality": {
                            "field": "eventId"
                        }
                    }
                }
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
        # pid = self.response["metricsRequest"]["filterBy"][0]["values"]
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
                records[month][i["key"]["country"]]["downloads"] = i["unique_doc_count"]["value"]
            if (i["key"]["format"] == "METADATA"):
                records[month][i["key"]["country"]]["views"] = i["unique_doc_count"]["value"]
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


    def gatherCitations(self, PIDs, metrics_database=None):
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

        start_date = "01/01/2012"
        end_date = datetime.today().strftime('%m/%d/%Y')

        data = metrics_elastic_search.iterate_composite_aggregations(search_query=search_body,
                                                                     aggregation_query=aggregation_body,
                                                                     start_date=datetime.strptime(start_date,
                                                                                                  '%m/%d/%Y'),
                                                                     end_date=datetime.strptime(end_date, '%m/%d/%Y'))

        # return {}, {}
        # return data, return_dict
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
                viewCount = views[target]["buckets"]["pid.key"]["unique_doc_count"]["value"]
            else:
                viewCount = 0
            if target in downloads:
                downloadCount = downloads[target]["buckets"]["pid.key"]["unique_doc_count"]["value"]
            else:
                downloadCount = 0
            while(target in obsoletesDictionary):
                target = obsoletesDictionary[target]
                if target in views:
                    viewCount += views[target]["buckets"]["pid.key"]["unique_doc_count"]["value"]
                if target in downloads:
                    downloadCount = downloads[target]["buckets"]["pid.key"]["unique_doc_count"]["value"]
            resultDict[i] = {}
            resultDict[i]["viewCount"] = viewCount
            resultDict[i]["downloadCount"] = downloadCount

        return resultDict


    def getMetricsPerRepository(self, nodeId):
        """
        Retrieves the metrics stats per repository
        Uses NodeID as repository ID
        :param: NodeId: Repository identifier to look up the metrics in the ES
        :return:
            Formatted Metrics Resonse object in JSON format
        """

        # Basic init for required objects
        t_start = time.time()
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()

        includeCitations, includeDownloads, includeViews = False, False, False
        if "citations" in self.response["metricsRequest"]["metrics"]:
            includeCitations = True
        if "downloads" in self.response["metricsRequest"]["metrics"]:
            includeDownloads = True
        if "views" in self.response["metricsRequest"]["metrics"]:
            includeViews = True

        t_delta = time.time() - t_start
        self.logger.debug('getMetricsPerRepository:t1=%.4f', t_delta)

        # Defaulting date to beginnning and end timestamps
        start_date = "01/01/2000"
        end_date = datetime.today().strftime('%m/%d/%Y')

        # update the date range if supplied in the query
        if (len(self.response["metricsRequest"]["filterBy"]) > 0):
            if ((self.response["metricsRequest"]["filterBy"][1]["filterType"] == "month" or
                    self.response["metricsRequest"]["filterBy"][1]["filterType"] == "day" or
                    self.response["metricsRequest"]["filterBy"][1]["filterType"] == "year") and
                    self.response["metricsRequest"]["filterBy"][1]["interpretAs"] == "range"):
                start_date = self.response["metricsRequest"]["filterBy"][1]["values"][0]
                end_date = self.response["metricsRequest"]["filterBy"][1]["values"][1]

        # Get the aggregation Type
        # default it to months
        aggType = "month"
        if (len(self.response["metricsRequest"]["groupBy"]) > 0):
            if "months" in self.response["metricsRequest"]["groupBy"]:
                aggType = "month"
            elif "days" in self.response["metricsRequest"]["groupBy"]:
                aggType = "day"
            elif "years" in self.response["metricsRequest"]["groupBy"]:
                aggType = "year"
        self.logger.debug('aggType: %s', aggType)

        data = {}
        if includeDownloads or includeViews:
            # defining the ES search and aggregation body for Repository profile
            search_body = [
                {
                    "term": {"event.key": "read"}
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

            if nodeId != "urn:node:CN":
                nodeTermQuery = {
                                    "term": {
                                        "nodeId": nodeId
                                    }
                                },
                search_body.append(nodeTermQuery)

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
                            },
                            {
                                aggType: {
                                    "date_histogram": {
                                        "field": "dateLogged",
                                        "interval": aggType
                                    }
                                }
                            }
                        ]
                    },
                    "aggs": {
                        "unique_doc_count": {
                            "cardinality": {
                                "field": "eventId"
                            }
                        }
                    }
                }
            }
            self.logger.debug('aggregation_body', aggregation_body)

            # if the aggregation is requested by country, add country object to groupBy
            if ("country" in self.response["metricsRequest"]["groupBy"]):
                countryObject = {
                    "country": {
                        "terms": {
                            "field": "geoip.country_code2.keyword",
                            "missing_bucket": "true"
                        }
                    }
                }
                aggregation_body["pid_list"]["composite"]["sources"].append(countryObject)

            # Query the ES with the designed Search and Aggregation body
            # uses the start_date and the end_date for the time range of data retrieval
            data = metrics_elastic_search.iterate_composite_aggregations(search_query=search_body,
                                                                         aggregation_query=aggregation_body,
                                                                         start_date=datetime.strptime(start_date,
                                                                                                      '%m/%d/%Y'),
                                                                         end_date=datetime.strptime(end_date,
                                                                                                    '%m/%d/%Y'))

        t_delta = time.time() - t_start
        self.logger.debug('getMetricsPerRepository:t3=%.4f', t_delta)

        node_list = []
        node_list.append(nodeId)

        return (self.formatElasticSearchResults(data, node_list, start_date, end_date, aggregationType=aggType, objectType="repository"))


    def getRepositoryCitationPIDs(self, nodeId):
        """

        :param nodeId:
        :return:
        """
        t_0 = time.time()
        self.logger.debug("enter getRepositoryCitationPIDs")
        metrics_database = MetricsDatabase()
        metrics_database.connect()
        csr = metrics_database.getCursor()
        if nodeId == "urn:node:CN":
            sql = 'SELECT target_id, origin, title, datePublished, dateUploaded, dateModified FROM citation_metadata;'
        else:
            sql = 'SELECT target_id, origin, title, datePublished, dateUploaded, dateModified FROM citation_metadata WHERE \''+ nodeId +'\' = ANY (node_id);'

        results = []
        target_citation_metadata = {}
        citationCount = 0
        try:
            csr.execute(sql)
            rows = csr.fetchall()
            for i in rows:
                results.append(i[0])
                target_citation_metadata[i[0]] = {}
                target_citation_metadata[i[0]]["origin"] = i[1]
                target_citation_metadata[i[0]]["title"] = i[2]
                target_citation_metadata[i[0]]["datePublished"] = i[3]
                target_citation_metadata[i[0]]["dateUploaded"] = i[4]
                target_citation_metadata[i[0]]["dateModified"] = i[5]
        except Exception as e:
            print('Database error!\n{0}', e)
        finally:
            pass
        self.logger.debug("exit getRepositoryCitationPIDs, elapsed=%fsec", time.time() - t_0)
        return results, target_citation_metadata


    def quoteTerm(self, term):
      '''
      Return a quoted, escaped Solr query term
      Args:
        term: (string) term to be escaped and quoted

      Returns: (string) quoted, escaped term
      '''
      return '"' + self.escapeSolrQueryTerm(term) + '"'


    def escapeSolrQueryTerm(self, term):
      '''
      Escape a solr query term for solr reserved characters
      Args:
        term: query term to be escaped

      Returns: string, the escaped query term
      '''
      term = term.replace('\\', '\\\\')
      for c in SOLR_RESERVED_CHAR_LIST:
        term = term.replace(c, '\{}'.format(c))
      return term


    def getMetricsPerUser(self, requestPIDArray):
        """
            Retrieves the metrics stats per user
            Uses set of dataset identifiers as userID for now
            :param: requestPIDArray - set of dataset identifiers that belongs to the user
            :return:
                Formatted Metrics Resonse object in JSON format
        """

        # Basic init for required objects
        t_start = time.time()
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()

        t_delta = time.time() - t_start
        self.logger.debug('getMetricsPerUser:t1=%.4f', t_delta)

        userPIDs = {}
        combinedPIDs = []
        for i in requestPIDArray:
            if i not in userPIDs:
                userPIDs[i] = []
                userPIDs[i].append(i)

        self.userPIDs = userPIDs

        t_resolve_start = time.time() - t_start

        return_dict = {}
        return_dict = pid_resolution.getResolvePIDs(userPIDs)

        t_resolve_end = time.time() - t_start

        for i in userPIDs:
            userPIDs[i] = return_dict[i]

        for i in userPIDs:
            combinedPIDs.extend(userPIDs[i])

        if (len(self.response["metricsRequest"]["filterBy"]) > 1):
            if (self.response["metricsRequest"]["filterBy"][1]["filterType"] == "month" and
                        self.response["metricsRequest"]["filterBy"][1]["interpretAs"] == "range"):
                start_date = self.response["metricsRequest"]["filterBy"][1]["values"][0]
            else:
                start_date = "01/01/2012"
        else:
            start_date = "01/01/2012"
        end_date = datetime.today().strftime('%m/%d/%Y')


        # Setting the query for the user profile
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
                    "sources": [
                        {
                            "format": {
                                "terms": {
                                    "field": "formatType"
                                }
                            }
                        },
                        {
                            "month": {
                                "date_histogram": {
                                    "field": "dateLogged",
                                    "interval": "month"
                                }
                            }
                        }
                    ]
                },
                "aggs": {
                    "unique_doc_count": {
                        "cardinality": {
                            "field": "eventId"
                        }
                    }
                }
            }
        }

        t_delta = time.time() - t_start
        self.logger.debug('getMetricsPerUser:t2=%.4f', t_delta)

        t_es_start = time.time() - t_start

        data = metrics_elastic_search.iterate_composite_aggregations(search_query=search_body,
                                                                     aggregation_query=aggregation_body,
                                                                     start_date=datetime.strptime(start_date,
                                                                                                  '%m/%d/%Y'),
                                                                     end_date=datetime.strptime(end_date, '%m/%d/%Y'))

        t_es_end = time.time() - t_start

        self.response["resolve_time"] = t_resolve_end - t_resolve_start
        self.response["es_time"] = t_es_end - t_es_start

        return (self.formatDataPerUser(data, combinedPIDs, start_date, end_date))


    def formatDataPerUser(self, data, citation_pids, start_date, end_date):
        """
        Formats the results retrieved from the Elastic Search and returns it as a HTTP response
        :param data: the data retrieve from ES
        :param citation_pids: PIDS to check the corresponding citations for
        :param start_date: begin date range for the results
        :param end_date: end date range for the results
        :return:
            A tuple of formatted JSON response objects containing the metrics corresponding metadata.
        """
        results = {
            "months": [],
            "downloads": [],
            "views": [],
            "citations": [],
        }

        # Getting the months between the two given dates:
        start = datetime.strptime(start_date, "%m/%d/%Y")
        end = datetime.strptime(end_date, "%m/%d/%Y")

        # Getting a list of all the months possible for the user
        # And initializing the corresponding metrics array
        results["months"] = list(
            OrderedDict(((start + timedelta(_)).strftime('%Y-%m'), None) for _ in range((end - start).days)).keys())
        results["downloads"] = [0] * len(results["months"])
        results["views"] = [0] * len(results["months"])
        results["citations"] = [0] * len(results["months"])

        # Gathering Citations
        resultDetails = {}
        resultDetails["citations"] = []
        citationDict = {}
        totalCitations, resultDetails["citations"] = self.gatherCitations(citation_pids)

        for citationObject in resultDetails["citations"]:
            if (citationObject["link_publication_date"][:7] in citationDict):
                citationDict[citationObject["link_publication_date"][:7]] = citationDict[
                                                                                citationObject["link_publication_date"][
                                                                                :7]] + 1
            else:
                citationDict[citationObject["link_publication_date"][:7]] = 1

        # Formatting the response from ES
        for i in data["aggregations"]["pid_list"]["buckets"]:
            months = datetime.utcfromtimestamp((i["key"]["month"] // 1000)).strftime(('%Y-%m'))
            month_index = results["months"].index(months)
            if i["key"]["format"] == "DATA":
                results["downloads"][month_index] += i["unique_doc_count"]["value"]
            elif i["key"]["format"] == "METADATA":
                results["views"][month_index] += i["unique_doc_count"]["value"]
            else:
                pass

        for months in citationDict:
            if months in results["months"]:
                month_index = results["months"].index(months)
                results["citations"][month_index] = citationDict[months]
            else:
                results["months"].append(months)
                results["views"].append(0)
                results["downloads"].append(0)
                results["citations"][month_index] = citationDict[months]

        return results, resultDetails


    def getMetricsPerGroup(self, groupPIDArray):
        """
            Retrieves the metrics stats per user
            Uses set of dataset identifiers as userID for now
            :param: requestPIDArray - set of dataset identifiers that belongs to the user
            :return:
                Formatted Metrics Resonse object in JSON format
        """

        # Basic init for required objects
        t_start = time.time()
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()

        t_delta = time.time() - t_start
        self.logger.debug('getMetricsPerGroup:t1=%.4f', t_delta)

        grouPID = groupPIDArray[0]
        print(grouPID)

        t_resolve_start = time.time() - t_start

        combinedPIDs = []
        combinedPIDs = self.getDatasetIdentifierFamily("group", grouPID)

        t_resolve_end = time.time() - t_start



        if (len(self.response["metricsRequest"]["filterBy"]) > 1):
            if (self.response["metricsRequest"]["filterBy"][1]["filterType"] == "month" and
                        self.response["metricsRequest"]["filterBy"][1]["interpretAs"] == "range"):
                start_date = self.response["metricsRequest"]["filterBy"][1]["values"][0]
            else:
                start_date = "01/01/2012"
        else:
            start_date = "01/01/2012"
        end_date = datetime.today().strftime('%m/%d/%Y')


        # Setting the query for the user profile
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
                    "sources": [
                        {
                            "format": {
                                "terms": {
                                    "field": "formatType"
                                }
                            }
                        },
                        {
                            "month": {
                                "date_histogram": {
                                    "field": "dateLogged",
                                    "interval": "month"
                                }
                            }
                        }
                    ]
                },
                "aggs": {
                    "unique_doc_count": {
                        "cardinality": {
                            "field": "eventId"
                        }
                    }
                }
            }
        }

        t_delta = time.time() - t_start
        self.logger.debug('getMetricsPerGroup:t2=%.4f', t_delta)
        self.logger.debug('getMetricsPerGroup:t2=%.4f', t_delta)

        t_es_start = time.time() - t_start

        data = metrics_elastic_search.iterate_composite_aggregations(search_query=search_body,
                                                                     aggregation_query=aggregation_body,
                                                                     start_date=datetime.strptime(start_date,
                                                                                                  '%m/%d/%Y'),
                                                                     end_date=datetime.strptime(end_date, '%m/%d/%Y'))

        t_es_end = time.time() - t_start

        self.response["resolve_time"] = t_resolve_end - t_resolve_start
        self.response["es_time"] = t_es_end - t_es_start
        self.response["combined_pids_length"] = len(combinedPIDs)

        return (self.formatDataPerGroup(data, combinedPIDs, start_date, end_date))


    def formatDataPerGroup(self, data, citation_pids, start_date, end_date):
        """
        Formats the results retrieved from the Elastic Search and returns it as a HTTP response
        :param data: the data retrieve from ES
        :param citation_pids: PIDS to check the corresponding citations for
        :param start_date: begin date range for the results
        :param end_date: end date range for the results
        :return:
            A tuple of formatted JSON response objects containing the metrics corresponding metadata.
        """
        results = {
            "months": [],
            "downloads": [],
            "views": [],
            "citations": [],
        }

        # Getting the months between the two given dates:
        start = datetime.strptime(start_date, "%m/%d/%Y")
        end = datetime.strptime(end_date, "%m/%d/%Y")

        # Getting a list of all the months possible for the user
        # And initializing the corresponding metrics array
        results["months"] = list(
            OrderedDict(((start + timedelta(_)).strftime('%Y-%m'), None) for _ in range((end - start).days)).keys())
        results["downloads"] = [0] * len(results["months"])
        results["views"] = [0] * len(results["months"])
        results["citations"] = [0] * len(results["months"])

        # Gathering Citations
        resultDetails = {}
        resultDetails["citations"] = []
        citationDict = {}
        totalCitations, resultDetails["citations"] = self.gatherCitations(citation_pids)

        for citationObject in resultDetails["citations"]:
            if (citationObject["link_publication_date"][:7] in citationDict):
                citationDict[citationObject["link_publication_date"][:7]] = citationDict[
                                                                                citationObject["link_publication_date"][
                                                                                :7]] + 1
            else:
                citationDict[citationObject["link_publication_date"][:7]] = 1

        # Formatting the response from ES
        for i in data["aggregations"]["pid_list"]["buckets"]:
            months = datetime.utcfromtimestamp((i["key"]["month"] // 1000)).strftime(('%Y-%m'))
            month_index = results["months"].index(months)
            if i["key"]["format"] == "DATA":
                results["downloads"][month_index] += i["unique_doc_count"]["value"]
            elif i["key"]["format"] == "METADATA":
                results["views"][month_index] += i["unique_doc_count"]["value"]
            else:
                pass

        for months in citationDict:
            if months in results["months"]:
                month_index = results["months"].index(months)
                results["citations"][month_index] = citationDict[months]
            else:
                results["months"].append(months)
                results["views"].append(0)
                results["downloads"].append(0)
                results["citations"][month_index] = citationDict[months]

        return results, resultDetails


    def getDatasetIdentifierFamily(self, filter_type, filter_type_identifier):
        """
        A method to query the new ES `identifiers-*` index
        :param filter_type:
        :param filter_type_identifier:
        :return:
        """

        # Basic init for required objects
        t_start = time.time()
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()

        t_delta = time.time() - t_start
        self.logger.debug('getMetricsPerUser:t1=%.4f', t_delta)

        if (filter_type == "dataset"):
            term_attribute = "_id"

            search_query = {
                "bool": {
                    "must": [
                        {
                            "term": {
                                term_attribute: filter_type_identifier
                            }
                        }
                    ]
                }
            }

            # Try searching the identifiers index for the datasetIdentifierFamily
            results = metrics_elastic_search.getDatasetIdentifierFamily(search_query)

            if len(results) > 0:
                for i in results[0]:
                    return (i["datasetIdentifierFamily"])

            else:

                print("No entry fround in identifier's index, so querying solr to resolve the corresponding pids")
                temp_array = []
                temp_array.append(filter_type_identifier)

                return (pid_resolution.getResolvePIDs(temp_array))

        elif (filter_type == "user" or filter_type == "group"):
            term_attribute = "userID.keyword"

            search_query = {
                "bool": {
                    "must": [
                        {
                            "term": {
                                term_attribute: filter_type_identifier
                            }
                        },
                        {
                            "term": {
                                "isPublic": "true"
                            }
                        }
                    ]
                }
            }

            # Try searching the identifiers index for the datasetIdentifierFamily
            results = metrics_elastic_search.getDatasetIdentifierFamily(search_query)

            self.response["hits"] = len(results[0])

            if len(results) > 0:
                combinedPIDs = []
                for i in results[0]:
                    combinedPIDs.extend(i["datasetIdentifierFamily"])
                return combinedPIDs


            else:

                print("No entry fround in identifier's index, so querying solr to resolve the corresponding pids")
                temp_array = []
                temp_array.append(filter_type_identifier)

                return (pid_resolution.getResolvePIDs(temp_array))


    def getPortalDatasetIdentifierFamily(self, portal_pids):
        """
            Gets the dataset identifier family for PIDs that belong to a specific potal

            :param: portal_pids Array object of dataset identifiers for a given portal
            :returns:
                Array of aggregated dataset identifier family for the datasets belonging to a portal
        """
        # Basic init for required objects
        t_start = time.time()
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()

        t_delta = time.time() - t_start
        self.logger.debug('getPortalDatasetIdentifierFamily:t1=%.4f', t_delta)
        datasetIdentifierFamily = []

        search_query = {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "PID.keyword": portal_pids
                        }
                    }
                ]
            }
        }

        # Try searching the identifiers index for the datasetIdentifierFamily
        results = metrics_elastic_search.getDatasetIdentifierFamily(search_query = search_query, index = "identifiers-2",  max_limit=10000)
        breakAt = 0

        for i in results[0]:
            for identifier in i["datasetIdentifierFamily"]:
                datasetIdentifierFamily.append(identifier)

        return(datasetIdentifierFamily, results[1])


    def getMetricsPerPortal(self, portalLabel, collectionQueryFilterObject=None):
        """
            Handles the Metrics generation for a given indexed portal
            :param: portal label
            :returns:
                Metrics Service response for the Metrics filter type 'portal'

        """
        # Setting the query for the user profile
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()

        results, resultDetails = {}, {}
        t_start = time.time()

        # Setting flags abse don the query params
        includeCitations, includeDownloads, includeViews = False, False, False
        if "citations" in self.response["metricsRequest"]["metrics"]:
            includeCitations = True
        if "downloads" in self.response["metricsRequest"]["metrics"]:
            includeDownloads = True
        if "views" in self.response["metricsRequest"]["metrics"]:
            includeViews = True

        # Defaulting date to beginnning and end timestamps
        start_date = "01/01/2000"
        end_date = datetime.today().strftime('%m/%d/%Y')

        # update the date range if supplied in the query
        if (len(self.response["metricsRequest"]["filterBy"]) > 0):
            if ((self.response["metricsRequest"]["filterBy"][1]["filterType"] == "month" or
                         self.response["metricsRequest"]["filterBy"][1]["filterType"] == "day" or
                         self.response["metricsRequest"]["filterBy"][1]["filterType"] == "year") and
                        self.response["metricsRequest"]["filterBy"][1]["interpretAs"] == "range"):
                start_date = self.response["metricsRequest"]["filterBy"][1]["values"][0]
                end_date = self.response["metricsRequest"]["filterBy"][1]["values"][1]

                # Get the aggregation Type
                # default it to months
                aggType = "month"
                if (len(self.response["metricsRequest"]["groupBy"]) > 0):
                    if "months" in self.response["metricsRequest"]["groupBy"]:
                        aggType = "month"
                    elif "days" in self.response["metricsRequest"]["groupBy"]:
                        aggType = "day"
                    elif "years" in self.response["metricsRequest"]["groupBy"]:
                        aggType = "year"
                self.logger.debug('aggType: %s', aggType)

        t_portal_dataset_identifier_family = time.time()

        # Get the portal's series identifier from Solr
        portalSeriesId = pid_resolution.getPortalSeriesId(portalLabel=portalLabel)
        pdif = []
        pdif.append(portalSeriesId)

        data = {}
        if includeDownloads or includeViews:
            # Search body for Portal Metrics
            search_body = [
                {
                    "term": {"event.key": "read"}
                },
                {
                    "term": {
                        "portalIdentifier.keyword": portalSeriesId
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

            # aggregation body for Portal Metrics
            aggregation_body = {
                "pid_list": {
                    "composite": {
                        "sources": [
                            {
                                "format": {
                                    "terms": {
                                        "field": "formatType"
                                    }
                                }
                            },
                            {
                                aggType: {
                                    "date_histogram": {
                                        "field": "dateLogged",
                                        "interval": aggType
                                    }
                                }
                            }
                        ]
                    },
                    "aggs": {
                        "unique_doc_count": {
                            "cardinality": {
                                "field": "eventId"
                            }
                        }
                    }
                }
            }

            # if the aggregation is requested by country, add country object to groupBy
            if ("country" in self.response["metricsRequest"]["groupBy"]):
                countryObject = {
                    "country": {
                        "terms": {
                            "field": "geoip.country_code2.keyword",
                            "missing_bucket": "true"
                        }
                    }
                }
                aggregation_body["pid_list"]["composite"]["sources"].append(countryObject)

            t_delta = time.time() - t_start
            self.logger.debug('getMetricsPerPortal:t2=%.4f', t_delta)

            t_es_start = time.time()

            # Query the ES with the designed Search and Aggregation body
            # uses the start_date and the end_date for the time range of data retrieval
            data = metrics_elastic_search.iterate_composite_aggregations(search_query=search_body,
                                                                         aggregation_query=aggregation_body,
                                                                         start_date=datetime.strptime(start_date,
                                                                                                      '%m/%d/%Y'),
                                                                         end_date=datetime.strptime(end_date,
                                                                                                    '%m/%d/%Y'))

        requestMetadata = {}
        requestMetadata["collectionDetails"] = resultDetails
        return (
        self.formatElasticSearchResults(data, pdif, start_date, end_date, aggregationType=aggType, objectType="portal",
                                        requestMetadata=requestMetadata))


    def getPortalCitationPIDs(self, seriesId):
        """
        Retrieves the cited datasets for a given portal series ID
        :param PID_List:
        :return:
        """
        t_0 = time.time()
        self.logger.debug("enter getPortalCitationPIDs")
        metrics_database = MetricsDatabase()
        metrics_database.connect()
        csr = metrics_database.getCursor()
        sql = "SELECT target_id, origin, title, datePublished, dateUploaded, dateModified FROM citation_metadata WHERE '" + seriesId + "' = ANY (portal_id);"

        results = []
        target_citation_metadata = {}
        citationCount = 0
        try:
            csr.execute(sql)
            rows = csr.fetchall()
            for i in rows:
                results.append(i[0])
                target_citation_metadata[i[0]] = {}
                target_citation_metadata[i[0]]["origin"] = i[1]
                target_citation_metadata[i[0]]["title"] = i[2]
                target_citation_metadata[i[0]]["datePublished"] = i[3]
                target_citation_metadata[i[0]]["dateUploaded"] = i[4]
                target_citation_metadata[i[0]]["dateModified"] = i[5]
        except Exception as e:
            print('Database error!\n{0}', e)
        finally:
            pass
        self.logger.debug("exit getPortalCitationPIDs, elapsed=%fsec", time.time() - t_0)
        return results, target_citation_metadata


    def formatElasticSearchResults(self, data, PIDList, start_date, end_date, aggregationType="month", objectType=None, requestMetadata={}):
        """
        Formats the ES response to the Metrics Service response
        Checks for groupBy requirements and utilzes appropriate functions.
        :param: data - Dictionary object retreieved as a response from ES
        :param: PIDList - List of identifiers associated with this request
        :param: start_date
        :param: end_date
        :param: objectType - Type of filter object (node, portal, user, group)
        :param: requestMetadata - additional metadata assoicated with the request.
        :return:
            results - Dictionary object for Metrics Response
            resultDetails - Dictionary object for additional information about the  Metrics Response
        """

        if aggregationType == "month":
            return (self.formatElasticSearchResultsByMonth(data, PIDList, start_date, end_date, objectType, requestMetadata))

        elif aggregationType == "day":
            return (self.formatElasticSearchResultsByDay(data, PIDList, start_date, end_date, objectType, requestMetadata))

        elif aggregationType == "year":
            return (self.formatElasticSearchResultsByYear(data, PIDList, start_date, end_date, objectType, requestMetadata))

        return {}, {}


    def formatElasticSearchResultsByMonth(self, data, PIDList, start_date, end_date, objectType=None, requestMetadata={}):
        """
        Formats the ES response to the Metrics Service response aggregated by month
        :param: data - Dictionary object retreieved as a response from ES
        :param: PIDList - List of identifiers associated with this request
        :param: start_date
        :param: end_date
        :param: objectType - Type of filter object (node, portal, user, group)
        :param: requestMetadata - additional metadata assoicated with the request.
        :return:
            results - Dictionary object for Metrics Response
            resultDetails - Dictionary object for additional information about the  Metrics Response
        """
        results = {
            "months": [],
            "downloads": [],
            "views": [],
            "citations": [],
            "country": [],
        }

        # Setting flags to filter out metrics based on the request
        if "citations" in self.response["metricsRequest"]["metrics"]:
            includeCitations = True
        else:
            includeCitations = False

        if "downloads" in self.response["metricsRequest"]["metrics"]:
            includeDownloads = True
        else:
            includeDownloads = False

        if "views" in self.response["metricsRequest"]["metrics"]:
            includeViews = True
        else:
            includeViews = False

        # Getting the months between the two given dates:
        start_dt = datetime.strptime(start_date, "%m/%d/%Y")
        end_dt = datetime.strptime(end_date, "%m/%d/%Y")

        # append totals to resultDetails object
        totalCitations, totalDownloads, totalViews = 0, 0, 0

        # Gathering Citations
        resultDetails = {}
        resultDetails["citations"] = []
        citationDict = {}
        resultDetailsCitationObject = []

        if includeCitations:
            if objectType == "repository":
                citation_pids, target_citation_metadata = self.getRepositoryCitationPIDs(PIDList[0])

            if objectType == "portal":
                resultDetails["collectionDetails"] = requestMetadata["collectionDetails"]
                citation_pids, target_citation_metadata = self.getPortalCitationPIDs(PIDList[0])

            totalCitationObjects, citationDetails = self.gatherCitations(citation_pids)

            for citationObject in citationDetails:
                citation_link_pub_date = citationObject["link_publication_date"][:7]

                # If citations publish date is not available, assign most recent month to it.
                if (citation_link_pub_date == None) or (citation_link_pub_date == "NULL"):
                    citation_link_pub_date = datetime.strftime(end_dt, "%Y-%m")

                # Check if the citations falls within the given time range.
                citation_pub_date = datetime.strptime(citation_link_pub_date, "%Y-%m")

                if (citation_pub_date > start_dt) and (citation_pub_date < end_dt):
                    resultDetailsCitationObject.append(citationObject)
                    totalCitations += 1
                    if (citation_link_pub_date in citationDict):
                        citationDict[citation_link_pub_date] = citationDict[citation_link_pub_date] + 1
                    else:
                        citationDict[citation_link_pub_date] = 1

        # Check if the request is for grouping by country
        if ("country" in self.response["metricsRequest"]["groupBy"]):
            # resultDetails["data"] = data["aggregations"]
            records = {}

            if includeDownloads or includeViews or data:
                # Combine metrics into a single dictionary
                for i in data["aggregations"]["pid_list"]["buckets"]:
                    month = datetime.utcfromtimestamp((i["key"]["month"] // 1000)).strftime(('%Y-%m'))

                    # handling cases where country is null
                    if (i["key"]["country"] is None) or (i["key"]["country"] == "null"):
                        i["key"]["country"] = "US"

                    if month not in records:
                        records[month] = {}
                    if i["key"]["country"] not in records[month]:
                        records[month][i["key"]["country"]] = {}
                    if (i["key"]["format"] == "DATA") and includeDownloads:
                        totalDownloads += i["unique_doc_count"]["value"]
                        records[month][i["key"]["country"]]["downloads"] = i["unique_doc_count"]["value"]
                    if (i["key"]["format"] == "METADATA") and includeViews:
                        totalViews += i["unique_doc_count"]["value"]
                        records[month][i["key"]["country"]]["views"] = i["unique_doc_count"]["value"]
                    pass

            # Parse the dictionary to form the expected output in the form of lists
            for months in records:
                for country in records[months]:
                    results["months"].append(months)
                    results["country"].append(country)
                    if includeDownloads:
                        if "downloads" in records[months][country]:
                            results["downloads"].append(records[months][country]["downloads"])
                        else:
                            results["downloads"].append(0)

                    # Views for the given time period.
                    if includeViews:
                        if "views" in records[months][country]:
                            results["views"].append(records[months][country]["views"])
                        else:
                            results["views"].append(0)

                    if includeCitations:
                        if (months in citationDict):
                            results["citations"].append(citationDict[months])
                        else:
                            results["citations"].append(0)

            if includeCitations:
                for months in citationDict:
                    if months not in results["months"]:
                        results["months"].append(months)

                        if includeViews:
                            results["views"].append(0)

                        if includeDownloads:
                            results["downloads"].append(0)

                        results["country"].append('US')
                        results["citations"].append(citationDict[months])

        # Proceed with the month facet by default for groupBy
        else:

            # Getting a list of all the months possible for the repo
            # And initializing the corresponding metrics array
            results["months"] = list(OrderedDict(
                ((start_dt + timedelta(_)).strftime('%Y-%m'), None) for _ in range((end_dt - start_dt).days)).keys())

            if includeDownloads:
                results["downloads"] = [0] * len(results["months"])

            if includeViews:
                results["views"] = [0] * len(results["months"])

            if includeCitations:
                results["citations"] = [0] * len(results["months"])

            if includeViews or includeDownloads or data:
                # Formatting the response from ES
                for i in data["aggregations"]["pid_list"]["buckets"]:
                    months = datetime.utcfromtimestamp((i["key"]["month"] // 1000)).strftime(('%Y-%m'))
                    month_index = results["months"].index(months)
                    if i["key"]["format"] == "DATA" and includeDownloads:
                        totalDownloads += i["unique_doc_count"]["value"]
                        results["downloads"][month_index] += i["unique_doc_count"]["value"]
                    elif i["key"]["format"] == "METADATA" and includeViews:
                        totalViews += i["unique_doc_count"]["value"]
                        results["views"][month_index] += i["unique_doc_count"]["value"]
                    else:
                        pass

            if includeCitations:
                for months in citationDict:
                    if months in results["months"]:
                        month_index = results["months"].index(months)
                        results["citations"][month_index] = citationDict[months]
                    else:
                        results["months"].append(months)

                        if includeDownloads:
                            results["downloads"].append(0)

                        if includeViews:
                            results["views"].append(0)

                        results["citations"][month_index] = citationDict[months]

        try:
            if includeCitations:
                # Returning citations and dataset links in resultDetails object
                targetSourceDict = {}
                for i in resultDetailsCitationObject:
                    if i["source_id"] not in targetSourceDict:
                        targetSourceDict[i["source_id"]] = {}
                        targetSourceDict[i["source_id"]]["target_id"] = []
                        targetSourceDict[i["source_id"]]["target_id"].append(i["target_id"])
                    else:
                        targetSourceDict[i["source_id"]]["target_id"].append(i["target_id"])
                    for k, v in i.items():
                        if k != "target_id":
                            targetSourceDict[i["source_id"]][k] = v
                    targetSourceDict[i["source_id"]]["citationMetadata"] = {}

                for source_id in targetSourceDict:
                    for each_target in targetSourceDict[source_id]["target_id"]:
                        targetSourceDict[source_id]["citationMetadata"][each_target] = {}
                        targetSourceDict[source_id]["citationMetadata"][each_target] = target_citation_metadata[each_target]
                resultDetails["citations"] = targetSourceDict
        except Exception as e:
            resultDetails["citations"] = {}
            self.logger.error(e)

        # append totals to the resultDetails object
        resultDetails["totalCitations"] = totalCitations
        resultDetails["totalDownloads"] = totalDownloads
        resultDetails["totalViews"] = totalViews
        resultDetails["aggType"] = "months"

        return results, resultDetails


    def formatElasticSearchResultsByDay(self, data, PIDList, start_date, end_date, objectType=None, requestMetadata={}):
        """
        Formats the ES response to the Metrics Service response aggregated by days
        :param: data - Dictionary object retreieved as a response from ES
        :param: PIDList - List of identifiers associated with this request
        :param: start_date
        :param: end_date
        :param: objectType - Type of filter object (node, portal, user, group)
        :param: requestMetadata - additional metadata assoicated with the request.
        :return:
            results - Dictionary object for Metrics Response
            resultDetails - Dictionary object for additional information about the  Metrics Response
        """
        results = {
            "days": [],
            "downloads": [],
            "views": [],
            "citations": [],
            "country": [],
        }

        # Setting flags to filter out metrics based on the request
        if "citations" in self.response["metricsRequest"]["metrics"]:
            includeCitations = True
        else:
            includeCitations = False

        if "downloads" in self.response["metricsRequest"]["metrics"]:
            includeDownloads = True
        else:
            includeDownloads = False

        if "views" in self.response["metricsRequest"]["metrics"]:
            includeViews = True
        else:
            includeViews = False

        # Setting the start date and end date provided
        start_dt = datetime.strptime(start_date, "%m/%d/%Y")
        end_dt = datetime.strptime(end_date, "%m/%d/%Y")

        # append totals to resultDetails object
        totalCitations, totalDownloads, totalViews = 0, 0, 0

        # Gathering Citations
        resultDetails = {}
        resultDetails["citations"] = []
        citationDict = {}
        resultDetailsCitationObject = []

        if includeCitations:
            if objectType == "repository":
                citation_pids, target_citation_metadata = self.getRepositoryCitationPIDs(PIDList[0])

            if objectType == "portal":
                resultDetails["collectionDetails"] = requestMetadata["collectionDetails"]
                citation_pids, target_citation_metadata = self.getPortalCitationPIDs(PIDList[0])

            totalCitationObjects, citationDetails = self.gatherCitations(citation_pids)

            for citationObject in citationDetails:
                citation_link_pub_date = citationObject["link_publication_date"]

                # If citations publish date is not available, assign most recent days to it.
                if (citation_link_pub_date == None) or (citation_link_pub_date == "NULL"):
                    citation_link_pub_date = datetime.strftime(end_dt, "%Y-%m-%d")

                # Check if the citations falls within the given time range.
                citation_pub_date = datetime.strptime(citation_link_pub_date, "%Y-%m-%d")
                if (citation_pub_date > start_dt) and (citation_pub_date < end_dt):
                    resultDetailsCitationObject.append(citationObject)
                    totalCitations += 1
                    if (citation_link_pub_date in citationDict):
                        citationDict[citation_link_pub_date] = citationDict[citation_link_pub_date] + 1
                    else:
                        citationDict[citation_link_pub_date] = 1

        # Check if the request is for grouping by country
        if ("country" in self.response["metricsRequest"]["groupBy"]):
            # resultDetails["data"] = data["aggregations"]
            records = {}

            if includeDownloads or includeViews or data:
                # Combine metrics into a single dictionary
                for i in data["aggregations"]["pid_list"]["buckets"]:
                    days = datetime.utcfromtimestamp((i["key"]["day"] // 1000)).strftime(('%Y-%m-%d'))

                    # handling cases where country is null
                    if (i["key"]["country"] is None) or (i["key"]["country"] == "null"):
                        i["key"]["country"] = "US"

                    if days not in records:
                        records[days] = {}
                    if i["key"]["country"] not in records[days]:
                        records[days][i["key"]["country"]] = {}
                    if (i["key"]["format"] == "DATA") and includeDownloads:
                        totalDownloads += i["unique_doc_count"]["value"]
                        records[days][i["key"]["country"]]["downloads"] = i["unique_doc_count"]["value"]
                    if (i["key"]["format"] == "METADATA") and includeViews:
                        totalViews += i["unique_doc_count"]["value"]
                        records[days][i["key"]["country"]]["views"] = i["unique_doc_count"]["value"]
                    pass

            # Parse the dictionary to form the expected output in the form of lists
            for days in records:
                for country in records[days]:
                    results["days"].append(days)
                    results["country"].append(country)
                    if includeDownloads:
                        if "downloads" in records[days][country]:
                            results["downloads"].append(records[days][country]["downloads"])
                        else:
                            results["downloads"].append(0)

                    # Views for the given time period.
                    if includeViews:
                        if "views" in records[days][country]:
                            results["views"].append(records[days][country]["views"])
                        else:
                            results["views"].append(0)

                    if includeCitations:
                        if (days in citationDict):
                            results["citations"].append(citationDict[days])
                        else:
                            results["citations"].append(0)

            if includeCitations:
                for days in citationDict:
                    if days not in results["days"]:
                        results["days"].append(days)

                        if includeViews:
                            results["views"].append(0)

                        if includeDownloads:
                            results["downloads"].append(0)

                        results["country"].append('US')
                        results["citations"].append(citationDict[days])

        # Proceed with the days facet by default for groupBy
        else:

            # Getting a list of all the days possible for the repo
            # And initializing the corresponding metrics array
            results["days"] = list(OrderedDict(
                ((start_dt + timedelta(_)).strftime('%Y-%m-%d'), None) for _ in range((end_dt - start_dt).days)).keys())

            if includeDownloads:
                results["downloads"] = [0] * len(results["days"])

            if includeViews:
                results["views"] = [0] * len(results["days"])

            if includeCitations:
                results["citations"] = [0] * len(results["days"])

            if includeViews or includeDownloads or data:
                # Formatting the response from ES
                for i in data["aggregations"]["pid_list"]["buckets"]:
                    days = datetime.utcfromtimestamp((i["key"]["day"] // 1000)).strftime(('%Y-%m-%d'))
                    day_index = results["days"].index(days)
                    if i["key"]["format"] == "DATA" and includeDownloads:
                        totalDownloads += i["unique_doc_count"]["value"]
                        results["downloads"][day_index] += i["unique_doc_count"]["value"]
                    elif i["key"]["format"] == "METADATA" and includeViews:
                        totalViews += i["unique_doc_count"]["value"]
                        results["views"][day_index] += i["unique_doc_count"]["value"]
                    else:
                        pass

            if includeCitations:
                for days in citationDict:
                    if days in results["days"]:
                        day_index = results["days"].index(days)
                        results["citations"][day_index] = citationDict[days]
                    else:
                        results["days"].append(days)

                        if includeDownloads:
                            results["downloads"].append(0)

                        if includeViews:
                            results["views"].append(0)

                        results["citations"][day_index] = citationDict[days]

        try:
            if includeCitations:
                # Returning citations and dataset links in resultDetails object
                targetSourceDict = {}
                for i in resultDetailsCitationObject:
                    if i["source_id"] not in targetSourceDict:
                        targetSourceDict[i["source_id"]] = {}
                        targetSourceDict[i["source_id"]]["target_id"] = []
                        targetSourceDict[i["source_id"]]["target_id"].append(i["target_id"])
                    else:
                        targetSourceDict[i["source_id"]]["target_id"].append(i["target_id"])
                    for k, v in i.items():
                        if k != "target_id":
                            targetSourceDict[i["source_id"]][k] = v
                    targetSourceDict[i["source_id"]]["citationMetadata"] = {}

                for source_id in targetSourceDict:
                    for each_target in targetSourceDict[source_id]["target_id"]:
                        targetSourceDict[source_id]["citationMetadata"][each_target] = {}
                        targetSourceDict[source_id]["citationMetadata"][each_target] = target_citation_metadata[
                            each_target]
                resultDetails["citations"] = targetSourceDict
        except Exception as e:
            resultDetails["citations"] = {}
            self.logger.error(e)

        # append totals to the resultDetails object
        resultDetails["totalCitations"] = totalCitations
        resultDetails["totalDownloads"] = totalDownloads
        resultDetails["totalViews"] = totalViews
        resultDetails["aggType"] = "days"

        return results, resultDetails


    def formatElasticSearchResultsByYear(self, data, PIDList, start_date, end_date, objectType=None, requestMetadata={}):
        """
        Formats the ES response to the Metrics Service response aggregated by years
        :param: data - Dictionary object retreieved as a response from ES
        :param: PIDList - List of identifiers associated with this request
        :param: start_date
        :param: end_date
        :param: objectType - Type of filter object (node, portal, user, group)
        :param: requestMetadata - additional metadata assoicated with the request.
        :return:
            results - Dictionary object for Metrics Response
            resultDetails - Dictionary object for additional information about the  Metrics Response
        """
        results = {
            "years": [],
            "downloads": [],
            "views": [],
            "citations": [],
            "country": [],
        }

        # Setting flags to filter out metrics based on the request
        if "citations" in self.response["metricsRequest"]["metrics"]:
            includeCitations = True
        else:
            includeCitations = False

        if "downloads" in self.response["metricsRequest"]["metrics"]:
            includeDownloads = True
        else:
            includeDownloads = False

        if "views" in self.response["metricsRequest"]["metrics"]:
            includeViews = True
        else:
            includeViews = False

        # Setting the start date and end date provided
        start_dt = datetime.strptime(start_date, "%m/%d/%Y")
        end_dt = datetime.strptime(end_date, "%m/%d/%Y")

        # append totals to resultDetails object
        totalCitations, totalDownloads, totalViews = 0, 0, 0

        # Gathering Citations
        resultDetails = {}
        resultDetails["citations"] = []
        citationDict = {}
        resultDetailsCitationObject = []

        if includeCitations:
            if objectType == "repository":
                citation_pids, target_citation_metadata = self.getRepositoryCitationPIDs(PIDList[0])

            if objectType == "portal":
                resultDetails["collectionDetails"] = requestMetadata["collectionDetails"]
                citation_pids, target_citation_metadata = self.getPortalCitationPIDs(PIDList[0])

            totalCitationObjects, citationDetails = self.gatherCitations(citation_pids)

            for citationObject in citationDetails:
                citation_link_pub_date = citationObject["link_publication_date"][:4]

                # If citations publish date is not available, assign most recent years to it.
                if (citation_link_pub_date == None) or (citation_link_pub_date == "NULL"):
                    citation_link_pub_date = datetime.strftime(end_dt, "%Y")

                # Check if the citations falls within the given time range.
                citation_pub_date = datetime.strptime(citation_link_pub_date, "%Y")
                if (citation_pub_date > start_dt) and (citation_pub_date < end_dt):
                    resultDetailsCitationObject.append(citationObject)
                    totalCitations += 1
                    if (citation_link_pub_date in citationDict):
                        citationDict[citation_link_pub_date] = citationDict[citation_link_pub_date] + 1
                    else:
                        citationDict[citation_link_pub_date] = 1

        # Check if the request is for grouping by country
        if ("country" in self.response["metricsRequest"]["groupBy"]):
            # resultDetails["data"] = data["aggregations"]
            records = {}

            if includeDownloads or includeViews or data:
                # Combine metrics into a single dictionary
                for i in data["aggregations"]["pid_list"]["buckets"]:
                    years = datetime.utcfromtimestamp((i["key"]["year"] // 1000)).strftime(('%Y'))

                    # handling cases where country is null
                    if (i["key"]["country"] is None) or (i["key"]["country"] == "null"):
                        i["key"]["country"] = "US"

                    if years not in records:
                        records[years] = {}
                    if i["key"]["country"] not in records[years]:
                        records[years][i["key"]["country"]] = {}
                    if (i["key"]["format"] == "DATA") and includeDownloads:
                        totalDownloads += i["unique_doc_count"]["value"]
                        records[years][i["key"]["country"]]["downloads"] = i["unique_doc_count"]["value"]
                    if (i["key"]["format"] == "METADATA") and includeViews:
                        totalViews += i["unique_doc_count"]["value"]
                        records[years][i["key"]["country"]]["views"] = i["unique_doc_count"]["value"]
                    pass

            # Parse the dictionary to form the expected output in the form of lists
            for years in records:
                for country in records[years]:
                    results["years"].append(years)
                    results["country"].append(country)
                    if includeDownloads:
                        if "downloads" in records[years][country]:
                            results["downloads"].append(records[years][country]["downloads"])
                        else:
                            results["downloads"].append(0)

                    # Views for the given time period.
                    if includeViews:
                        if "views" in records[years][country]:
                            results["views"].append(records[years][country]["views"])
                        else:
                            results["views"].append(0)

                    if includeCitations:
                        if (years in citationDict):
                            results["citations"].append(citationDict[years])
                        else:
                            results["citations"].append(0)

            if includeCitations:
                for years in citationDict:
                    if years not in results["years"]:
                        results["years"].append(years)

                        if includeViews:
                            results["views"].append(0)

                        if includeDownloads:
                            results["downloads"].append(0)

                        results["country"].append('US')
                        results["citations"].append(citationDict[years])

        # Proceed with the years facet by default for groupBy
        else:

            # Getting a list of all the years possible for the repo
            # And initializing the corresponding metrics array
            results["years"] = list(OrderedDict(
                ((start_dt + timedelta(_)).strftime('%Y'), None) for _ in range((end_dt - start_dt).days)).keys())

            if includeDownloads:
                results["downloads"] = [0] * len(results["years"])

            if includeViews:
                results["views"] = [0] * len(results["years"])

            if includeCitations:
                results["citations"] = [0] * len(results["years"])

            if includeViews or includeDownloads or data:
                # Formatting the response from ES
                for i in data["aggregations"]["pid_list"]["buckets"]:
                    years = datetime.utcfromtimestamp((i["key"]["year"] // 1000)).strftime(('%Y'))
                    year_index = results["years"].index(years)
                    if i["key"]["format"] == "DATA" and includeDownloads:
                        totalDownloads += i["unique_doc_count"]["value"]
                        results["downloads"][year_index] += i["unique_doc_count"]["value"]
                    elif i["key"]["format"] == "METADATA" and includeViews:
                        totalViews += i["unique_doc_count"]["value"]
                        results["views"][year_index] += i["unique_doc_count"]["value"]
                    else:
                        pass

            if includeCitations:
                for years in citationDict:
                    if years in results["years"]:
                        year_index = results["years"].index(years)
                        results["citations"][year_index] = citationDict[years]
                    else:
                        results["years"].append(years)

                        if includeDownloads:
                            results["downloads"].append(0)

                        if includeViews:
                            results["views"].append(0)

                        results["citations"][year_index] = citationDict[years]

        try:
            if includeCitations:
                # Returning citations and dataset links in resultDetails object
                targetSourceDict = {}
                for i in resultDetailsCitationObject:
                    if i["source_id"] not in targetSourceDict:
                        targetSourceDict[i["source_id"]] = {}
                        targetSourceDict[i["source_id"]]["target_id"] = []
                        targetSourceDict[i["source_id"]]["target_id"].append(i["target_id"])
                    else:
                        targetSourceDict[i["source_id"]]["target_id"].append(i["target_id"])
                    for k, v in i.items():
                        if k != "target_id":
                            targetSourceDict[i["source_id"]][k] = v
                    targetSourceDict[i["source_id"]]["citationMetadata"] = {}

                for source_id in targetSourceDict:
                    for each_target in targetSourceDict[source_id]["target_id"]:
                        targetSourceDict[source_id]["citationMetadata"][each_target] = {}
                        targetSourceDict[source_id]["citationMetadata"][each_target] = target_citation_metadata[
                            each_target]
                resultDetails["citations"] = targetSourceDict
        except Exception as e:
            resultDetails["citations"] = {}
            self.logger.error(e)

        # append totals to the resultDetails object
        resultDetails["totalCitations"] = totalCitations
        resultDetails["totalDownloads"] = totalDownloads
        resultDetails["totalViews"] = totalViews
        resultDetails["aggType"] = "years"

        return results, resultDetails


if __name__ == "__main__":
    mr = MetricsReader()
    # mr.resolvePIDs(["doi:10.5065/D6BG2KW9"])
    mr.getDatasetIdentifierFamily("user", "http://orcid.org/0000-0002-0381-3766")

