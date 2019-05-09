'''
Implements a wrapper for the metrics reporting service.
'''

from elasticsearch import Elasticsearch
from elasticsearch import helpers

import argparse
import sys
import requests
import json
import urllib.request
from xml.etree import ElementTree
from datetime import datetime
from datetime import timedelta
from urllib.parse import quote_plus
from d1_metrics.metricselasticsearch import MetricsElasticSearch
from collections import Counter
from dateutil.relativedelta import relativedelta
import logging
import asyncio

DEFAULT_REPORT_CONFIGURATION={
    "report_url" : "https://api.datacite.org/reports",
    "auth_token" : "",
    "report_name" : "Dataset Master Report",
    "release" : "rd1",
    "created_by" : "DataONE",
    "solr_query_url": "https://cn.dataone.org/cn/v2/query/solr/"
}


class MetricsReporter(object):

    def __init__(self):
        self._config = DEFAULT_REPORT_CONFIGURATION
        self.logger = logging.getLogger('metrics_reporting_service.' + __name__)
        self.logger.setLevel(logging.DEBUG)

        # create file handler which logs even debug messages
        fh = logging.FileHandler('./reports/reports.log')
        fh.setLevel(logging.DEBUG)

        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)

        # create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # add the handlers to the logger
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)


    def report_handler(self, start_date, end_date, node, unique_pids):
        """
        Creates a Report JSON object, dumps it to a file and sends the report to the Hub.
        This is a handler function that manages the entire work flow
        :param start_date:
        :param end_date:
        :param: node
        :param: unique_pids
        :return: None
        """
        json_object = {}
        json_object["report-header"] = self.get_report_header(start_date, end_date, node)
        json_object["report-datasets"] = self.get_report_datasets(start_date, end_date, unique_pids, node)
        with open('./reports/' + ("DSR-D1-" + (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d'))+ "-" + node+'.json', 'w') as outfile:
            json.dump(json_object, outfile, indent=2,ensure_ascii=False)
        response = self.send_reports(start_date, end_date, node)
        return response


    def get_report_header(self, start_date, end_date, node):
        """
        Generates a unique report header
        :param start_date:
        :param end_date:
        :return: Dictionary report header object.
        """
        report_header = {}
        report_header["report-name"] = self._config["report_name"]
        report_header["report-id"] = "dsr"
        report_header["release"] = self._config["release"]
        report_header["reporting-period"] = {}
        report_header["reporting-period"]["begin-date"] = (datetime.strptime(start_date,'%m/%d/%Y')).strftime('%Y-%m-%d')
        report_header["reporting-period"]["end-date"] = (datetime.strptime(end_date, '%m/%d/%Y')).strftime(
            '%Y-%m-%d')
        report_header["created"] = datetime.now().strftime('%Y-%m-%d')
        report_header["created-by"] = node
        report_header["report-filters"] = []
        report_header["report-attributes"] = []

        return (report_header)


    def get_unique_pids(self, start_date, end_date, node, doi=False):
        """
        Queries ES for the given time period and returns the set of pids
        :param start_date:
        :param end_date:
        :param: node
        :return: SET object of pids for a given time range. (Always unique - because it is a set!)
        """
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        pid_list = []
        unique_pids = []
        query = [
            {
                "terms": {
                    "formatType": [
                        "METADATA"
                    ]
                }
            },
            {

                "term": {"event.key": "read"}
            },
            {
                "term": {"nodeId": node}
            },
            {
                "exists": {
                    "field": "sessionId"
                }
            }
        ]

        # Just search for DOI string in to send it to the HUB
        if(doi):
            DOIWildcard = {}
            DOIWildcard["wildcard"] = {"pid.key": "*doi*"}
            query.append(DOIWildcard)

        fields = "pid"
        results, total = metrics_elastic_search.getSearches(limit=1000000, q = query, date_start=datetime.strptime(start_date,'%m/%d/%Y')\
                                                     , date_end=datetime.strptime(end_date,'%m/%d/%Y'), fields=fields)

        for i in range(total):
            pid_list.append(results[i]["pid"])

        for i in pid_list:
            if i not in unique_pids:
                unique_pids.append(i)

        return (unique_pids)


    def generate_instances(self, start_date, end_date, pid_list):
        """

        :param start_date:
        :param end_date:
        :return:
        """
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        report_instances = {}
        search_body = [
            {
                "terms": {
                    "pid.key": pid_list
                }
            },
            {
                "terms": {
                    "formatType": [
                        "METADATA",
                        "DATA"
                    ]
                }
            },
            {
                "term": {"event.key": "read"}
            },
            {
                "exists": {
                    "field": "sessionId"
                }
            }
        ]
        aggregation_body = {
            "pid_list": {
                "composite": {
                    "size": 100,
                    "sources": [
                        {
                            "session": {
                                "terms": {
                                    "field": "sessionId"
                                }
                            }
                        },
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
        data = metrics_elastic_search.iterate_composite_aggregations(search_query=search_body, aggregation_query = aggregation_body,\
                                                                     start_date=datetime.strptime(start_date,'%m/%d/%Y'),\
                                                                     end_date=datetime.strptime(end_date,'%m/%d/%Y'))


        for i in data["aggregations"]["pid_list"]["buckets"]:
            if(i["key"]["format"] == "METADATA"):
                if "METADATA" in report_instances:
                    report_instances["METADATA"]["unique_investigations"] = report_instances["METADATA"]["unique_investigations"] + 1
                    report_instances["METADATA"]["total_investigations"] = report_instances["METADATA"][
                                                                                "total_investigations"] + i["doc_count"]
                    if (i["key"]["country"]).lower() in report_instances["METADATA"]["country_unique_investigations"]:
                        report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] = \
                            report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] + 1
                        report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] = \
                        report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] + i["doc_count"]
                    else:
                        report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] = 1
                        report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] = i["doc_count"]
                else:
                    report_instances["METADATA"] = {
                        "unique_investigations" : 1,
                        "total_investigations": i["doc_count"],
                        "country_unique_investigations": {
                            (i["key"]["country"]).lower(): 1
                        },
                        "country_total_investigations": {
                            (i["key"]["country"]).lower(): i["doc_count"]
                        }
                    }
            if (i["key"]["format"] == "DATA"):
                if "DATA" in report_instances:
                    report_instances["METADATA"]["unique_investigations"] = report_instances["METADATA"][
                                                                                "unique_investigations"] + 1
                    report_instances["METADATA"]["total_investigations"] = report_instances["METADATA"][
                                                                               "total_investigations"] + i["doc_count"]
                    report_instances["DATA"]["unique_requests"] = report_instances["DATA"][
                                                                                "unique_requests"] + 1
                    report_instances["DATA"]["total_requests"] = report_instances["DATA"][
                                                                               "total_requests"] + i["doc_count"]
                    if (i["key"]["country"]).lower() in report_instances["DATA"]["country_unique_requests"]:
                        report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] = \
                            report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] + 1
                        report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] = \
                            report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] + i[
                                "doc_count"]
                        report_instances["DATA"]["country_unique_requests"][(i["key"]["country"]).lower()] = \
                            report_instances["DATA"]["country_unique_requests"][(i["key"]["country"]).lower()] + 1
                        report_instances["DATA"]["country_total_requests"][(i["key"]["country"]).lower()] = \
                            report_instances["DATA"]["country_total_requests"][(i["key"]["country"]).lower()] + i[
                                "doc_count"]
                    else:
                        if (i["key"]["country"]).lower() in report_instances["METADATA"]["country_unique_investigations"]:
                            report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] = \
                                report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] + 1
                            report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] = \
                                report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] + i[
                                    "doc_count"]
                        else:
                            report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] = 1
                            report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] = i[
                                "doc_count"]
                        report_instances["DATA"]["country_unique_requests"][(i["key"]["country"]).lower()] = 1
                        report_instances["DATA"]["country_total_requests"][(i["key"]["country"]).lower()] = i[
                            "doc_count"]
                else:
                    if "METADATA" in report_instances:
                        report_instances["METADATA"]["unique_investigations"] = report_instances["METADATA"][
                                                                                    "unique_investigations"] + 1
                        report_instances["METADATA"]["total_investigations"] = report_instances["METADATA"][
                                                                                   "total_investigations"] + i[
                                                                                   "doc_count"]
                        if (i["key"]["country"]).lower() in report_instances["METADATA"]["country_unique_investigations"]:
                            report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] = \
                                report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] + 1
                            report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] = \
                                report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] + i[
                                    "doc_count"]
                        else:
                            report_instances["METADATA"]["country_unique_investigations"][(i["key"]["country"]).lower()] = 1
                            report_instances["METADATA"]["country_total_investigations"][(i["key"]["country"]).lower()] = i[
                                "doc_count"]
                    else:
                        report_instances["METADATA"] = {
                            "unique_investigations": 1,
                            "total_investigations": i["doc_count"],
                            "country_unique_investigations": {
                                (i["key"]["country"]).lower(): 1
                            },
                            "country_total_investigations": {
                                (i["key"]["country"]).lower(): i["doc_count"]
                            }
                        }
                    report_instances["DATA"] = {
                        "unique_requests": 1,
                        "total_requests": i["doc_count"],
                        "country_unique_requests": {
                            (i["key"]["country"]).lower(): 1
                        },
                        "country_total_requests": {
                            (i["key"]["country"]).lower(): i["doc_count"]
                        }
                    }
        return report_instances


    def get_report_datasets(self, start_date, end_date, unique_pids, node ):
        """

        :param start_date:
        :param end_date:
        :param: unique_pids
        :param: node
        :return:
        """
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        report_datasets = []
        pid_list = []


        count = 0
        nodeName = self.resolve_MN(node)
        self.logger.debug("Processing " + str(len(unique_pids)) + " datasets for node " + node)
        for pid in unique_pids:
            count = count + 1
            if((count % 100 == 0) or (count == 1) or (count == len(unique_pids))) :
                self.logger.debug(str(count) + " of "  + str(len(unique_pids)))


            dataset = {}
            solr_response = self.query_solr(pid)
            if(solr_response["response"]["numFound"] > 0):

                if ("title" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["dataset-title"] = solr_response["response"]["docs"][0]["title"]
                else:
                    dataset["dataset-title"] = ""

                if ("authoritativeMN" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["publisher"] = self.resolve_MN(solr_response["response"]["docs"][0]["authoritativeMN"])
                else:
                    dataset["publisher"].append(
                        {"type": "urn", "value": nodeName})

                if ("authoritativeMN" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["publisher-id"] = []
                    dataset["publisher-id"].append({"type":"urn", "value" :solr_response["response"]["docs"][0]["authoritativeMN"]})
                else:
                    dataset["publisher-id"].append(
                        {"type": "urn", "value": node})

                dataset["platform"] = "DataONE"

                if ("origin" in (i for i in solr_response["response"]["docs"][0])):
                    contributors = []
                    for i in solr_response["response"]["docs"][0]["origin"]:
                        contributors.append({"type": "name", "value": i})
                    dataset["dataset-contributors"] = contributors

                if ("datePublished" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["dataset-dates"] = []
                    dataset["dataset-dates"].append({"type": "pub-date", "value" :solr_response["response"]["docs"][0]["datePublished"][:10]})
                else:
                    dataset["dataset-dates"] = []
                    dataset["dataset-dates"].append({"type": "pub-date", "value" :solr_response["response"]["docs"][0]["dateUploaded"][:10]})

                if "doi" in pid:
                    dataset["dataset-id"] = [{"type": "doi", "value": pid}]
                else:
                    continue
                    # dataset["dataset-id"] = [{"type": "other-id", "value": pid}]

                dataset["yop"] = dataset["dataset-dates"][0]["value"][:4]

                if ("dataUrl" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["uri"] = solr_response["response"]["docs"][0]["dataUrl"]

                dataset["data-type"] = "dataset"

                dataset["performance"] = []
                performance = {}

                performance["period"] = {}
                performance["period"]["begin-date"] = (datetime.strptime(start_date,'%m/%d/%Y')).strftime('%Y-%m-%d')
                performance["period"]["end-date"] = (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d')

                instance = []
                pid_list = []
                pid_list.append(pid)
                pid_list = self.resolvePIDs(pid_list)

                report_instances = self.generate_instances(start_date, end_date, pid_list)

                if ("METADATA" in report_instances):
                    total_dataset_investigation = {"count": report_instances["METADATA"]["total_investigations"],
                                                   "access-method": "regular",
                                                   "metric-type": "total-dataset-investigations",
                                                   "country-counts": report_instances["METADATA"]["country_total_investigations"]}

                    unique_dataset_investigation = {"count": report_instances["METADATA"]["unique_investigations"],
                                                    "access-method": "regular",
                                                    "metric-type": "unique-dataset-investigations",
                                                    "country-counts": report_instances["METADATA"]["country_unique_investigations"]}
                    instance.append(total_dataset_investigation)
                    instance.append(unique_dataset_investigation)


                if("DATA" in report_instances):
                    total_dataset_requests = {"count": report_instances["DATA"]["total_requests"],
                                              "access-method": "regular",
                                              "metric-type": "total-dataset-requests",
                                              "country-counts": report_instances["DATA"]["country_total_requests"]}

                    unique_dataset_requests = {"count": report_instances["DATA"]["unique_requests"],
                                               "access-method": "regular",
                                               "metric-type": "unique-dataset-requests",
                                               "country-counts": report_instances["DATA"]["country_unique_requests"]}
                    instance.append(total_dataset_requests)
                    instance.append(unique_dataset_requests)

                performance["instance"] = instance

                dataset["performance"].append(performance)

            else:
                continue

            report_datasets.append(dataset)

        return (report_datasets)


    def resolve_MN(self, authoritativeMN):
        """
        Queries the Node endpoint to retrieve the details about the authoritativeMN
        :param authoritativeMN:
        :return: String value of the name of the authoritativeMN
        """
        node_url = "https://cn.dataone.org/cn/v2/node/" + authoritativeMN
        resp = requests.get(node_url, stream=True)
        root = ElementTree.fromstring(resp.content)
        name = root.find('name').text
        return name


    def resolvePIDs(self, PIDs):
        """
        Checks for the versions and obsolecence chain of the given PID
        :param PID:
        :return: A list of pids for previous versions and their data + metadata objects
        """

        # get the ids for all the previous versions and their data / metadata object till the current `pid` version
        # p.s. this might not be the latest version!
        callSolr = True

        while (callSolr):

            # Querying for all the PIDs that we got from the previous iteration
            # Would be a single PID if this is the first iteration.
            identifier = '(("' + '") OR ("'.join(PIDs) + '"))'

            # Forming the query dictionary to be sent as a file to the Solr endpoint via the HTTP Post request.
            queryDict =  {}
            queryDict["fq"] = (None, 'id:' + identifier)
            queryDict["fl"] =  (None, 'id,documents,documentedBy,obsoletes,resourceMap')
            queryDict["wt"] = (None, "json")

            # Getting length of the array from previous iteration to control the loop
            prevLength = len(PIDs)

            resp = requests.post(url=self._config["solr_query_url"], files=queryDict)

            if(resp.status_code == 200):

                response = resp.json()

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

            if (prevLength == len(PIDs)):
                callSolr = False
        return PIDs


    def send_reports(self,  start_date, end_date, node):
        """
        Sends report to the Hub at the specified Hub report url in the config parameters
        :return: Nothing
        """
        s = requests.session()
        s.headers.update(
            {'Authorization': "Bearer " +  self._config["auth_token"], 'Content-Type': 'application/json', 'Accept': 'application/json'})
        with open("./reports/DSR-D1-" + (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d')+ "-" + node+'.json', 'r') as content_file:
            content = content_file.read()
        response = s.post(self._config["report_url"], data=content.encode("utf-8"))

        return response


    def query_solr(self, PID):
        """
        Queries the Solr end-point for metadata given the PID.
        :param PID:
        :return: JSON Object containing the metadata fields queried from Solr
        """

        queryString = 'q=id:"' + PID + '"&fl=origin,title,datePublished,dateUploaded,authoritativeMN,dataUrl&wt=json'
        response = requests.get(url = self._config["solr_query_url"], params = queryString)

        return response.json()


    def scheduler(self):
        """
        This function sends reports to the hub with events reported on daily basis from Jan 01, 2000
        Probably would be called only once in its lifetime
        :return: None
        """
        mn_list = self.get_MN_List()
        for node in mn_list:
            date = datetime(2013, 1, 1)
            stopDate = datetime(2013, 12, 31)

            count = 0
            while (date.strftime('%Y-%m-%d') != stopDate.strftime('%Y-%m-%d')):
                self.logger.debug("Running job for Node: " + node)

                count = count + 1

                prevDate = date + timedelta(days=1)
                date = self.last_day_of_month(prevDate)

                start_date, end_date = prevDate.strftime('%m/%d/%Y'),\
                             date.strftime('%m/%d/%Y')

                unique_pids = self.get_unique_pids(start_date, end_date, node, doi=True)

                if (len(unique_pids) > 0):
                    self.logger.debug("Job " + " : " + start_date + " to " + end_date)

                    # Uncomment me to send reports to the HUB!
                    response = self.report_handler(start_date, end_date, node, unique_pids)


                    logentry = "Node " + node + " : " + start_date + " to " + end_date + " === " + str(response.status_code)

                    self.logger.debug(logentry)

                    if response.status_code != 201:

                        logentry = "Node " + node + " : " + start_date + " to " + end_date + " === " \
                                   + str(response.status_code)
                        self.logger.error(logentry)
                        self.logger.error(str(response.status_code) + " " + response.reason)
                        self.logger.error("Headers: " + str(response.headers))
                        self.logger.error("Content: " + str((response.content).decode("utf-8")))
                else:
                    self.logger.debug(
                        "Skipping job for " + node + " " + start_date + " to " + end_date + " - length of PIDS : " + str(
                            len(unique_pids)))


    def last_day_of_month(self, date):
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month + 1, day=1) - timedelta(days=1)


    def get_MN_List(self):
        """
        Retreives a MN idenifier from the https://cn.dataone.org/cn/v2/node/ endpoint
        Used to send the reports for different MNs
        :return: Set of Member Node identifiers
        """
        node_url = "https://cn.dataone.org/cn/v2/node/"
        resp = requests.get(node_url, stream=True)
        root = ElementTree.fromstring(resp.content)
        mn_list = set()
        for child in root:
            node_type = child.attrib['type']
            identifier = child.find('identifier')
            if (node_type == "mn"):
                mn_list.add(identifier.text)
        return(mn_list)


    def get_es_unique_dois(self, start_date, end_date, nodeId = None):
        """

        Finds the dois from the eventlog and
        returns a dictionary with the doi as the key and it's corresponding PID as a value

        :param start_date: begin date for search
        :param end_date: end date for search
        :param nodeId: Node ID for the query term

        :return: dictionary object

        """

        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()



        seriesIdWildCard = {
            "seriesId": {
                "value": "*doi*"
            }
        }

        PIDWildCard = {
            "pid.key": {
                "value": "*doi*"
            }
        }

        doi_dict = {}

        query = {
            "bool": {
                "must": [
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
                    },
                    {
                        "wildcard": seriesIdWildCard
                    }
                ]
            }
        }

        if not nodeId:
            nodeQuery = {
                "term": {
                    "nodeId" : nodeId
                }
            }
            query["bool"]["must"].append(nodeQuery)

        fields = ["pid", "seriesId"]

        results, total1 = metrics_elastic_search.getSearches(limit=1000000, q=query, fields=fields, date_start=datetime.strptime(start_date,'%m/%d/%Y')
                                                     , date_end=datetime.strptime(end_date,'%m/%d/%Y'))

        print(total1 , " == ", len(results))

        for result in results:
            if result["seriesId"] not in doi_dict:
                doi_dict[result["seriesId"]] = []
                doi_dict[result["seriesId"]].append(result["pid"])

        query["bool"]["must"][3]["wildcard"] = PIDWildCard

        results, total2 = metrics_elastic_search.getSearches(limit=1000000, q=query, fields=fields, date_start=datetime.strptime(start_date,'%m/%d/%Y')
                                                     , date_end=datetime.strptime(end_date,'%m/%d/%Y'))

        print(total2, " == ", len(results))

        for result in results:
            if result["pid"] not in doi_dict:
                doi_dict[result["pid"]] = []
                doi_dict[result["pid"]].append(result["pid"])

        return self.get_dataset_identifier_family(doi_dict)


    def get_doi_dict_dataset_identifier_family(self, doi_dict):
        """
        Gets the dataset_identifier_family from the identifiers index for every key in the doi_dict

        :param: doi_dict
            A dictionary of the DOIs with the doi as the key and the resolved_pids a.k.a
            the dataset_identifier_family as the value

        :return: dictionary object
        """

        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()


        def _get_dataset_identifier_family(pid):
            """

            Retrieves the dataset_identifier_family

            :param pid: The PID of interest

            :return: a dictionary object

            """

            result = {}
            result[pid] = []
            result[pid].append(pid)

            query_body = {
                "bool": {
                    "should": [
                        {
                            "term": {
                                "PID.keyword": pid
                            }
                        }
                    ]
                }
            }

            data = metrics_elastic_search.getDatasetIdentifierFamily(search_query=query_body, max_limit=1)

            result[pid].extend(data[0]["datasetIdentifierFamily"])

            return result


        async def work_get_identifier_family(doi_dict):
            """

            Creates async task to query ES, executes those tasks and returns results to
            the parent function

            :param doi_dict:

            :return:

            """

            with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
                loop = asyncio.get_event_loop()
                tasks = []

                # for every pid in the dict
                # cerate a new task and add it to the task list
                for an_id, val in doi_dict:
                    tasks.append(loop.run_in_executor(executor, _get_dataset_identifier_family, an_id))

                # wait for the response to complete the tasks
                for response in await asyncio.gather(*tasks):
                    results[response[0]] = response

        results = {}

        # In a multithreading environment such as under gunicorn, the new thread created by
        # gevent may not provide an event loop. Create a new one if necessary.
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError as e:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        future = asyncio.ensure_future(work_get_identifier_family(doi_dict))

        # wait for the work to complete
        loop.run_until_complete(future)

        print(results)

        return results


if __name__ == "__main__":
  md = MetricsReporter()
  # md.get_report_header("01/20/2018", "02/20/2018")
  # md.get_report_datasets("05/01/2018", "05/31/2018")
  # md.resolve_MN("urn:node:KNB")
  # md.query_solr("df35b.302.1")
  # md.report_handler("05/01/2018", "05/30/2018")
  # md.get_unique_pids("05/01/2018", "05/31/2018")
  md.scheduler()
  # md.resolvePIDs(["doi:10.18739/A2X65H"])