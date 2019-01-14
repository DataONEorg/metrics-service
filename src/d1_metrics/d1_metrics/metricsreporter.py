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
import logging
import gzip
import traceback
import asyncio
import concurrent.futures
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pprint import  pprint
from aiohttp import ClientSession
from time import sleep
from xml.etree import ElementTree
from datetime import datetime
from datetime import timedelta
from urllib.parse import quote_plus
from d1_metrics.metricselasticsearch import MetricsElasticSearch
from d1_metrics_service import pid_resolution
from d1_metrics.metricsreportutilities import MetricsReportUtilities
from collections import Counter
from dateutil.relativedelta import relativedelta

DEFAULT_REPORT_CONFIGURATION={
    "report_url" : "https://api.datacite.org/reports/",
    "auth_token" : "",
    "report_name" : "Dataset Master Report",
    "release" : "rd1",
    "created_by" : "DataONE",
    "solr_query_url": "https://cn.dataone.org/cn/v2/query/solr/",
    "solr_query_url_2" : "	https://cn-unm-1.dataone.org/cn/v2/query/solr/",
    "solr_query_url_3" : "	https://cn-orc-1.dataone.org/cn/v2/query/solr/"
}
CONCURRENT_REQUESTS = 20

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

        # adding gzip specific exceptions
        # as we'll be sending gzipped reports if the contents had more than a 1000 datasets
        if(len(unique_pids) > 1000):
            report_exception = {
                "code": 69,
                "severity": "warning",
                "message": "Report is compressed using gzip",
                "help-url": "https://github.com/datacite/sashimi",
                "data": "usage data needs to be uncompressed"
            }
            json_object["report-header"]["exceptions"].append(report_exception)


        # Creating a JSON file
        with open('./reports/' + ("DSR-D1-" + (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d'))+ "-" + node+'.json', 'w') as outfile:
            json.dump(json_object, outfile, indent=2,ensure_ascii=False)

        # Sending reports to the HUB
        response = self.send_reports(start_date, end_date, node, len(unique_pids))

        return response


    def get_report_header(self, start_date, end_date, node):
        """
        Generates a unique report header
        :param start_date:
        :param end_date:
        :return: Dictionary report header object.
        """
        # Forming the report_header object
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
        report_header["exceptions"] = []

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

        # Parsing Elastic Search aggregation results
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

        # resolved pids dictionary for entire unique_pids list
        PIDDict = {}

        current_resolve_count = 0
        total_resolve_count = 0
        temp_pid_list = []
        count = 0


        nodeName = self.resolve_MN(node)
        self.logger.debug("Processing " + str(len(unique_pids)) + " datasets for node " + node)

        for pid in unique_pids:
            current_resolve_count += 1
            total_resolve_count += 1
            temp_pid_list.append(pid)

            if ((current_resolve_count == 500) or (total_resolve_count == len(unique_pids))):
                # create
                exception_count = 0
                keep_trying = True

                while ((keep_trying == True) and (exception_count < 2)):

                    try:
                        PIDDict.update(pid_resolution.getResolvePIDs(temp_pid_list))
                        temp_pid_list = []
                        current_resolve_count = 0
                        keep_trying = False

                    except Exception as e:
                        # any other exceptions
                        keep_trying = True
                        exception_count += 1
                        self.logger.debug(
                            "Try # " + str(exception_count))
                        self.logger.error(traceback.format_exc())

            if ((total_resolve_count % 500 == 0) or (total_resolve_count == 1) or (total_resolve_count == len(unique_pids))):
                self.logger.debug("AsyncIO processed " + str(total_resolve_count) + " out of " + str(len(unique_pids)) + " datasets.")

        self.PIDDict = PIDDict

        PIDs = []
        current_generated_count = 0
        total_generated_count = 0
        count = 0
        for pid in unique_pids:
            current_generated_count += 1
            total_generated_count += 1
            PIDs.append(pid)
            if ((current_generated_count % 500 == 0) or (total_generated_count == len(unique_pids))):
                exception_count = 0
                keep_trying = True

                while ((keep_trying == True) and (exception_count < 2)):
                    try:

                        report_datasets.extend(self.generate_report_datasets(PIDs, start_date, end_date, nodeName, node))
                        PIDs = []
                        current_generated_count = 0
                        keep_trying = False

                    except Exception as e:
                        keep_trying = True
                        exception_count += 1
                        self.logger.debug(
                            "Try # " + str(exception_count))
                        self.logger.error(traceback.format_exc())

            if ((total_generated_count % 500 == 0) or (total_generated_count == 1) or (total_generated_count == len(unique_pids))):
                self.logger.debug("AsyncIO Generating Instance " + str(total_generated_count) + " of " + str(len(unique_pids)))

        return (report_datasets)


    def generate_report_datasets(self, PIDs, start_date, end_date, nodeName, node):
        """
        Given a  set of unique dataset identifiers- this method generates dataset instances asynchronously
        :param PIDs: Unique list of dataset identifiers
        :param start_date: Begin date of the report
        :param end_date: End date of the report
        :param nodeName: Resolved name of the node that reported this dataset read event
        :param node: Identifier of the node that reported this dataset read event
        :return: List of dataset objects in SUHSI format
        """

        def _fetch(self, query_url, pid, start_date, end_date, nodeName, node, sess = None):
            """
            This method forms a dataset object using the metadata retreived from multiple sources (SOLR and ES)
            for a single pid
            :param PIDs: Unique list of dataset identifiers
            :param start_date: Begin date of the report
            :param end_date: End date of the report
            :param nodeName: Resolved name of the node that reported this dataset read event
            :param node: Identifier of the node that reported this dataset read event
            :return: A well formatted dataset object to be included in the report
            """

            dataset = {}
            solr_response = self.query_solr(pid, query_url, sess = sess)

            if (solr_response["response"]["numFound"] > 0):

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
                    dataset["publisher-id"].append(
                        {"type": "urn", "value": solr_response["response"]["docs"][0]["authoritativeMN"]})
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
                    dataset["dataset-dates"].append(
                        {"type": "pub-date", "value": solr_response["response"]["docs"][0]["datePublished"][:10]})
                else:
                    dataset["dataset-dates"] = []
                    dataset["dataset-dates"].append(
                        {"type": "pub-date", "value": solr_response["response"]["docs"][0]["dateUploaded"][:10]})

                if "doi" in pid:
                    dataset["dataset-id"] = [{"type": "doi", "value": pid}]
                else:
                    return {}
                    # dataset["dataset-id"] = [{"type": "other-id", "value": pid}]

                dataset["yop"] = dataset["dataset-dates"][0]["value"][:4]

                if ("dataUrl" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["uri"] = solr_response["response"]["docs"][0]["dataUrl"]

                dataset["data-type"] = "dataset"

                dataset["performance"] = []
                performance = {}

                performance["period"] = {}
                performance["period"]["begin-date"] = (datetime.strptime(start_date, '%m/%d/%Y')).strftime('%Y-%m-%d')
                performance["period"]["end-date"] = (datetime.strptime(end_date, '%m/%d/%Y')).strftime('%Y-%m-%d')

                instance = []
                pid_list = self.PIDDict[pid]

                report_instances = self.generate_instances(start_date, end_date, pid_list)

                if ("METADATA" in report_instances):
                    total_dataset_investigation = {"count": report_instances["METADATA"]["total_investigations"],
                                                   "access-method": "regular",
                                                   "metric-type": "total-dataset-investigations",
                                                   "country-counts": report_instances["METADATA"][
                                                       "country_total_investigations"]}

                    unique_dataset_investigation = {"count": report_instances["METADATA"]["unique_investigations"],
                                                    "access-method": "regular",
                                                    "metric-type": "unique-dataset-investigations",
                                                    "country-counts": report_instances["METADATA"][
                                                        "country_unique_investigations"]}
                    instance.append(total_dataset_investigation)
                    instance.append(unique_dataset_investigation)

                if ("DATA" in report_instances):
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
                pass

            return dataset

        async def _work(self, pids, start_date, end_date, nodeName, node):
            """
            Given a set of unique identifiers schedules asynchronous jobs to retreive metadata
            :param pids: set of unique identifiers
            :param start_date: Begin date of the report
            :param end_date: End date of the report
            :param nodeName: Resolved name of the node that reported this dataset read event
            :param node: Identifier of the node that reported this dataset read event
            :return: List of well formed dataset objects for every unique pid passed via the pids list
            """

            session = requests.Session()
            retry = Retry(connect=3, backoff_factor=0.5)
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('https://', adapter)

            with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:

                loop = asyncio.get_event_loop()
                tasks = []

                fetch_count = 0
                for an_id in pids:
                    fetch_count += 1
                    query_url = self.solr_round_robin(fetch_count)
                    tasks.append(loop.run_in_executor(executor, _fetch, self, query_url, an_id, start_date, end_date, nodeName, node, session ))

                for response in await asyncio.gather(*tasks):
                    results.append(response)

        results = []
        t_start = time.time()

        self.logger.debug("Entering _work")

        # In a multithreading environment such as under gunicorn, the new thread created by
        # gevent may not provide an event loop. Create a new one if necessary.
        try:
            loop = asyncio.get_event_loop()

        except RuntimeError as e:
            self.logger.info("Creating new event loop.")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        future = asyncio.ensure_future(_work(self, PIDs, start_date, end_date, nodeName, node))
        loop.run_until_complete(future)

        self.logger.debug("elapsed:%fsec", time.time() - t_start)

        return results


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


    def send_reports(self, start_date, end_date, node, report_length):
        """
        Sends report to the Hub at the specified Hub report url in the config parameters
        - If the number of datasets in the report are less than 5000 send a JSON report
        - else send a gzipped report

        :param: start_date - the starting range of dataset events included in the reports
        :param: end_date - the starting range of dataset events included in the reports
        :param: node - the authoritative MN the report was generated for
        :param: report_length The number of datasets included in this report
        :return: HTTP response from the hub
        """
        s = requests.session()

        # Send an unzipped report to the hub
        if (report_length < 1000):

            s.headers.update(
                {'Authorization': "Bearer " + self._config["auth_token"], 'Content-Type': 'application/json',
                 'Accept': 'application/json'})

            with open("./reports/DSR-D1-" + (datetime.strptime(end_date, '%m/%d/%Y')).strftime(
                    '%Y-%m-%d') + "-" + node + '.json', 'r') as content_file:
                content = content_file.read()

            response = s.post(self._config["report_url"], data=content.encode("utf-8"))

        # Send a gzipped report if there are too many datasets within a report
        else:
            s.headers.update(
                {'Authorization': "Bearer " + self._config["auth_token"], 'Content-Type': 'application/gzip',
                 'Accept': 'application/gzip', 'Content-Encoding': 'gzip'})

            with open("./reports/DSR-D1-" + (datetime.strptime(end_date, '%m/%d/%Y')).strftime(
                    '%Y-%m-%d') + "-" + node + '.json', 'r') as content_file:
                # JSON large object data
                jlob = content_file.read()

                # JSON large object bytes
                jlob = jlob.encode("utf-8")

                with open("./reports/DSR-D1-" + (datetime.strptime(end_date, '%m/%d/%Y')).strftime(
                    '%Y-%m-%d') + "-" + node + ".gzip", mode="wb") as f:
                    f.write(gzip.compress(jlob))

            response = s.post(self._config["report_url"], data=gzip.compress(jlob))

        return response


    def query_solr(self, PID, query_url = None, sess = None):
        """
        Queries the Solr end-point for metadata given the PID.
        :param PID: The dataset identifier used to retreive the metadata
        :param query_url: The url used to query the solr index
        :param sess: Requests's session
        :return: JSON Object containing the metadata fields queried from Solr
        """
        if sess is None:
            self.logger.info("Session is null!")
            sess = requests.Session()

        queryString = 'q=id:"' + PID + '"&fl=origin,title,datePublished,dateUploaded,authoritativeMN,dataUrl&wt=json'
        if query_url is None:
            query_url = self._config["solr_query_url"]
        response = sess.get(url = query_url, params = queryString)

        return response.json()


    def scheduler(self):
        """
        This function sends reports to the hub with events reported on daily basis from Jan 01, 2000
        Probably would be called only once in its lifetime
        :return: None
        """
        mn_list = self.get_MN_List()

        #already sent reports
        util = MetricsReportUtilities()
        sent_dict = {}
        self.logger.debug(
            "Getting previously submitted reports from DataONE")
        sent_dict = util.get_created_reports()

        for node in mn_list:
            self.logger.debug("Running job for Node: " + node)
            date = datetime(2015, 1, 1)
            stopDate = datetime(2015, 12, 31)

            count = 0
            while (date.strftime('%Y-%m-%d') != stopDate.strftime('%Y-%m-%d')):

                count = count + 1

                if (date.month == 1 or count == 1):
                    prevDate = date
                else:
                    prevDate = date + timedelta(days=1)

                sent_dates = []
                try:
                    sent_dates = sent_dict[node]
                except KeyError as e:
                    pass

                date = self.last_day_of_month(prevDate)

                start_date, end_date = prevDate.strftime('%m/%d/%Y'),\
                             date.strftime('%m/%d/%Y')

                unique_pids = self.get_unique_pids(start_date, end_date, node, doi=True)

                if (len(unique_pids) > 0 and len(unique_pids) < 50000):
                    if (prevDate.strftime('%Y-%m-%d') in sent_dates):
                        self.logger.debug((prevDate.strftime('%Y-%m-%d') + " in " + str(sent_dates)))
                        self.logger.debug(
                            "Report already sent for: " + node + " start_date at - " + date.strftime('%Y-%m-%d'))
                        continue
                    else:
                        self.logger.debug((prevDate.strftime('%Y-%m-%d') + " not in " + str(sent_dates)))

                    self.logger.debug("Job " + " : " + start_date + " to " + end_date)

                    # Uncomment me to send reports to the HUB!
                    response = self.report_handler(start_date, end_date, node, unique_pids)

                    logentry = "Node " + node + " : " + start_date + " to " + end_date + " === " + str(response.status_code)

                    self.logger.debug(logentry)

                    if response.status_code != 201:
                        self.logger.error(str(response.status_code) + " " + response.reason)
                        self.logger.error("Headers: " + str(response.headers))
                        self.logger.error("Content: " + str((response.content).decode("utf-8")))
                else:
                    self.logger.debug(
                        "Skipping job for " + node + " " + start_date + " to " + end_date + " - length of PIDS : " + str(
                            len(unique_pids)))


    def last_day_of_month(self, date):
        """
        Simple utility funciton
        Returns the last day of the month to set the end_date range for report generation
        :param date:
        :return:
        """
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month + 1, day=1) - timedelta(days=1)


    def get_MN_List(self):
        """
        Retreives a MN idenifiers from the https://cn.dataone.org/cn/v2/node/ endpoint
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

    def solr_round_robin(self, iter_count):
        """
        Round robin implementation to distribute jobs events within the number of solr instances
        :param iter_count: The iteration count for the job to determine which solr instance to query
        :return:
        """
        solr_instances = []
        solr_instances.append(self._config["solr_query_url"])
        solr_instances.append(self._config["solr_query_url_2"])
        solr_instances.append(self._config["solr_query_url_3"])


        return solr_instances[iter_count % len(solr_instances)]


if __name__ == "__main__":
  md = MetricsReporter()
  # md.get_report_header("01/20/2018", "02/20/2018")
  # md.get_report_datasets("05/01/2018", "05/31/2018")
  # md.resolve_MN("urn:node:KNB")
  # md.query_solr("df35b.302.1")
  # md.report_handler("05/01/2018", "05/30/2018")
  # md.get_unique_pids("05/01/2018", "05/31/2018")
  # md.resolvePIDs(["doi:10.18739/A2X65H"])
  md.scheduler()