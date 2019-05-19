'''
Implements a wrapper for the metrics reporting service.
'''

from elasticsearch import Elasticsearch
from elasticsearch import helpers
import argparse
import sys
import time
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
import gzip
import shutil
import asyncio
from aiohttp import ClientSession
import concurrent.futures
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

DEFAULT_REPORT_CONFIGURATION={
    "report_url" : "https://api.datacite.org/reports",
    "auth_token" : "",
    "report_name" : "Dataset Master Report",
    "release" : "rd1",
    "created_by" : "DataONE",
    "solr_query_url": "https://cn.dataone.org/cn/v2/query/solr/"
}

CONCURRENT_REQUESTS = 10  #max number of concurrent requests to run

class MetricsReporter(object):

    def __init__(self):
        self._config = DEFAULT_REPORT_CONFIGURATION

        self.es = MetricsElasticSearch()
        self.es.connect()

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


    def report_handler(self, start_date, end_date, node, doi_dict, perform_put=None):
        """
        Creates a Report JSON object, dumps it to a file and sends the report to the Hub.
        This is a handler function that manages the entire work flow
        :param start_date:
        :param end_date:
        :param: node
        :param: doi_dict
        :return: None
        """
        json_object = {}


        large_report = False
        if len(doi_dict) > 2000:
            large_report = True

        json_object["report-header"] = self.get_report_header(start_date, end_date, node, large_report)
        report_datasets = self.get_async_report_datasets(start_date, end_date, node, doi_dict)

        if report_datasets:
            json_object["report-datasets"] = self.get_async_report_datasets(start_date, end_date, node, doi_dict)

            with open('./reports/' + ("DSR-D1-" + (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d'))+ "-" + node+'.json', 'w') as outfile:
                json.dump(json_object, outfile, indent=2, ensure_ascii=False)

            if len(doi_dict) > 2000:
                response = response = self.send_reports(start_date, end_date, node, perform_put, compressed=large_report)
            else:
                response = response = self.send_reports(start_date, end_date, node, perform_put, compressed=large_report)

            return response

        return None


    def get_report_header(self, start_date, end_date, node, large_report):
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

        if large_report:
            report_header["exceptions"] = [
                {
                    "code": 69,
                    "severity": "warning",
                    "message": "Report is compressed using gzip",
                    "help-url": "https://github.com/datacite/sashimi",
                    "data": "usage data needs to be uncompressed"
                }
            ]
        else:
            report_header["exceptions"] = []

        return (report_header)


    def generate_instances(self, start_date, end_date, pid_list):
        """

        :param start_date:
        :param end_date:
        :return:
        """
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
                                    "field": "geoip.country_code2.keyword",
                                    "missing_bucket":"true"
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
        data = self.es.iterate_composite_aggregations(search_query=search_body, aggregation_query = aggregation_body,\
                                                                     start_date=datetime.strptime(start_date,'%m/%d/%Y'),\
                                                                     end_date=datetime.strptime(end_date,'%m/%d/%Y'))


        for i in data["aggregations"]["pid_list"]["buckets"]:
            if i["key"]["country"] is None:
                i["key"]["country"] = "n/a"
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


    def get_async_report_datasets(self, start_date, end_date, node, doi_dict):
        """
        Generates asynchronous dataset instances.

        :param start_date:
            Start date of the report
        :param end_date:
            End date of the report
        :param: node
            Node identifier for the logs from ES
        :param: doi_dict
            Dictionary object with dois and their datasetIdentifierFamily

        :return: list object
            list of dataset instances as defined in SUSHI format
        """

        time_beg = time.time()

        count = 0
        mn_dict = self.get_MN_Dict()
        nodeName = mn_dict[node]

        def _get_single_dataset_instance(self, doi, pid_list):
            """
            Generartes a single instance of dataset object

            :param self:
                The self object
            :param doi:
                The doi for the dataset
            :param pid_list:
                `datasetIdentifierFamily` for this doi

            :return: dictionary object
                a single dataset instance as defined in SUSHI format
            """

            dataset = {}
            pid = pid_list[0]
            solr_response = self.query_solr(pid)
            if (solr_response["response"]["numFound"] > 0):

                if ("title" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["dataset-title"] = solr_response["response"]["docs"][0]["title"]
                else:
                    return None

                if ("authoritativeMN" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["publisher"] = mn_dict[solr_response["response"]["docs"][0]["authoritativeMN"]]
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

                if doi:
                    dataset["dataset-id"] = [{"type": "doi", "value": doi}]
                else:
                    return None
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

                for i in instance:
                    if "n/a" in i["country-counts"]:
                        i["country-counts"].pop("n/a", None)

                performance["instance"] = instance

                dataset["performance"].append(performance)

            else:
                return None

            return dataset

        async def _work_get_all_datasets_instances(self, doi_dict):
            """
            For all the PIDs in the doi_dict, create async jobs and execute them concurrently

            :param self:
                Class object
            :param doi_dict:
                Dictionary of `doi` as key and `datasetIdentifierFamily` as value

            :return:
                None
            """

            with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
                self.logger.info("self.async init : %f sec", time.time() - time_beg)
                loop = asyncio.get_event_loop()
                tasks = []

                for pid, pid_list in doi_dict.items():
                    tasks.append(loop.run_in_executor(executor, _get_single_dataset_instance, self, pid, pid_list))

                for response in await asyncio.gather(*tasks):
                    if len(tasks) % 100 == 0:
                        self.logger.info(len(tasks))
                    if response is not None:
                        report_datasets.append(response)

                self.logger.info("self.async end : %f sec", time.time() - time_beg)

        report_datasets = []

        self.logger.info("self.get_es_unique_dois : %f sec", time.time() - time_beg)
        self.logger.debug("Processing " + str(len(doi_dict)) + " datasets for node " + node)

        # In a multithreading environment such as under gunicorn, the new thread created by
        # gevent may not provide an event loop. Create a new one if necessary.
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError as e:
            _L.info("Creating new event loop.")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        future = asyncio.ensure_future(_work_get_all_datasets_instances(self, doi_dict))
        loop.run_until_complete(future)

        if len(report_datasets) < 1:
            return None

        self.logger.info("total : %f sec", time.time() - time_beg)

        return (report_datasets)


    def send_reports(self, start_date, end_date, node, perform_put=None, compressed=False):
        """
        Sends reports to the Hub at the specified Hub report url in the config parameters.

        The DataCite HUB has a limit on the number of dataset instance that it can inject.
        If ther reports are too large, it gives errors while injesting the reports.

        To handle cases with large reports the `compressed` parameters should be set to True.

        :param: start_date
            String object representing the beginning of the report

        :param: end_date
            String object representing the end interval of the report

        :param: node
            The corresponding node to which the report belong to

        :param: compressed
            A boolean parameter that represents whether to send zipped reports or not

        :return: response
            A HTTP reponse object reporesenting the status of the sent zipped report
        """
        s = requests.session()

        name = "./reports/DSR-D1-" + (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d')+ "-" + node

        if compressed:

            s.headers.update(
                {'Authorization': "Bearer " +  self._config["auth_token"], 'Content-Type': 'application/gzip', 'Accept': 'gzip', 'Content-Encoding': 'gzip'})

            with open(name + ".json", 'r') as content_file:
                # JSON large object data
                jlob = content_file.read()

                # JSON large object bytes
                jlob = jlob.encode("utf-8")

                with open(name + ".gzip", mode="wb") as f:
                    f.write(jlob)

            if perform_put:
                self.logger.info("Performing PUT")
                response = s.put(self._config["report_url"] + "/" + perform_put, data=gzip.compress(jlob))

            response = s.post(self._config["report_url"], data=gzip.compress(jlob))

        else:
            s.headers.update(
                {'Authorization': "Bearer " + self._config["auth_token"], 'Content-Type': 'application/json',
                 'Accept': 'application/json'})

            with open(name + '.json', 'r') as content_file:
                content = content_file.read()

            if perform_put:
                self.logger.info("Performing PUT")
                response = s.put(self._config["report_url"] + "/" + perform_put, data=content.encode("utf-8"))

            response = s.post(self._config["report_url"], data=content.encode("utf-8"))

        self.logger.info(response)
        self.logger.info(str(response.status_code) + " " + response.reason)
        self.logger.info("Headers: " + str(response.headers))
        self.logger.info("Content: " + str((response.content).decode("utf-8")))

        return response


    def query_solr(self, PID):
        """
        Queries the Solr end-point for metadata given the PID.
        :param PID:
        :return: JSON Object containing the metadata fields queried from Solr
        """

        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        queryString = 'q=id:"' + PID + '"&fl=origin,title,datePublished,dateUploaded,authoritativeMN,dataUrl&wt=json'
        response = session.get(url = self._config["solr_query_url"], params = queryString)

        return response.json()


    def scheduler(self):
        """
        This function sends reports to the hub with events reported on daily basis from Jan 01, 2000
        Probably would be called only once in its lifetime
        :return: None
        """
        mn_dict = self.get_MN_Dict()
        for node, nodeName in mn_dict.items():
            self.logger.debug("Running job for Node: " + node)

            date = datetime(2012, 6, 30)
            stopDate = datetime(2014, 12, 31)

            jobs_done = self.get_jobs_done(node)

            count = 0
            while (date.strftime('%Y-%m-%d') != stopDate.strftime('%Y-%m-%d')):
                count = count + 1

                prevDate = date + timedelta(days=1)
                date = self.last_day_of_month(prevDate)

                start_date, end_date = prevDate.strftime('%m/%d/%Y'),\
                             date.strftime('%m/%d/%Y')

                perform_put = None
                job_done_date = prevDate.strftime('%Y-%m-%d')
                if job_done_date in jobs_done:
                    perform_put = jobs_done[job_done_date]


                orignial_doi_dict = self.get_es_unique_dois(start_date, end_date, nodeId = node)

                doi_dict = {k: orignial_doi_dict[k] for k in list(orignial_doi_dict)[:100]}

                if (len(doi_dict) > 0):
                    self.logger.info("Job " + " : " + start_date + " to " + end_date)

                    # Uncomment me to send reports to the HUB!
                    response = self.report_handler(start_date, end_date, node, doi_dict, perform_put)

                    if response is None:
                        self.logger.info(
                            "Skipping job for " + node + " " + start_date + " to " + end_date + " - no datasets to submit!")
                        continue

                    logentry = "Node " + node + " : " + start_date + " to " + end_date + " === " + str(response.status_code)

                    self.logger.info(logentry)

                    if response.status_code != 201:

                        logentry = "Node " + node + " : " + start_date + " to " + end_date + " === " \
                                   + str(response.status_code)
                        self.logger.error(logentry)
                        self.logger.error(str(response.status_code) + " " + response.reason)
                        self.logger.error("Headers: " + str(response.headers))
                        self.logger.error("Content: " + str((response.content).decode("utf-8")))
                else:
                    self.logger.info(
                        "Skipping job for " + node + " " + start_date + " to " + end_date + " - length of PIDS : " + str(
                            len(doi_dict)))


    def last_day_of_month(self, date):
        """
        Returns the last day of the month for report generation

        :param date:
            A date object to get the last date of that month

        :return: date object
            Last day of the month for the date instance supplied in the parameter
        """
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month + 1, day=1) - timedelta(days=1)


    def get_MN_Dict(self, mn = True):
        """
        Retreives a MN idenifier from the https://cn.dataone.org/cn/v2/node/ endpoint
        Used to send the reports for different MNs

        :return: Dictionary of Member Node identifiers
            Key - MN identifier
            Value - Full name of the MN

        """
        node_url = "https://cn.dataone.org/cn/v2/node/"
        resp = requests.get(node_url, stream=True)
        root = ElementTree.fromstring(resp.content)
        mn_dict = dict()

        for child in root:
            if child.get('type') == "mn" and mn:
                identifier = child.find('identifier').text
                name = child.find('name').text
                mn_dict[identifier] = name
            else:
                identifier = child.find('identifier').text
                name = child.find('name').text
                mn_dict[identifier] = name

        return (mn_dict)


    def get_es_unique_dois(self, start_date, end_date, nodeId = None):
        """

        Finds the dois from the eventlog and
        returns a dictionary with the doi as the key and it's corresponding PID as a value

        :param start_date: begin date for search
        :param end_date: end date for search
        :param nodeId: Node ID for the query term

        :return: dictionary object

        """

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
            },
            {
                "wildcard": seriesIdWildCard
            }
        ]

        if nodeId:
            nodeQuery = {
                "term": {
                    "nodeId" : nodeId
                }
            }
            search_body.append(nodeQuery)

        fields = ["pid", "seriesId"]

        results, total1 = self.es.getSearches (q=search_body, index='eventlog-1', limit=1000000, fields=fields,
                                                             date_start=datetime.strptime(start_date,'%m/%d/%Y'),
                                                             date_end=datetime.strptime(end_date,'%m/%d/%Y'))

        for result in results:
            if result["seriesId"] not in doi_dict:
                doi_dict[result["seriesId"]] = []
                doi_dict[result["seriesId"]].append(result["pid"])

        search_body[3]["wildcard"] = PIDWildCard

        results, total2 = self.es.getSearches(q=search_body, index='eventlog-1', limit=1000000, fields=fields,
                                                             date_start=datetime.strptime(start_date,'%m/%d/%Y'),
                                                             date_end=datetime.strptime(end_date,'%m/%d/%Y'))

        for result in results:
            if result["pid"] not in doi_dict:
                doi_dict[result["pid"]] = []
                doi_dict[result["pid"]].append(result["pid"])

        # query identifiers index only if there is anything to query!
        if len(doi_dict) > 0:
            return self.get_doi_dict_dataset_identifier_family(doi_dict)

        return {}


    def get_doi_dict_dataset_identifier_family(self, doi_dict):
        """
        Gets the dataset_identifier_family from the identifiers index for every key in the doi_dict

        :param: doi_dict
            A dictionary of the DOIs with the doi as the key and the resolved_pids a.k.a
            the dataset_identifier_family as the value

        :return: dictionary object
        """


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

            data = self.es.getDatasetIdentifierFamily(search_query=query_body, max_limit=1)

            # Parse only if there are existing records found in the `identifiers index`
            try:
                if data[1] > 0:
                    result[pid].extend(data[0][0]["datasetIdentifierFamily"])
            except:
                pass

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
                for an_id in doi_dict:
                    tasks.append(loop.run_in_executor(executor, _get_dataset_identifier_family, an_id))

                # wait for the response to complete the tasks
                for response in await asyncio.gather(*tasks):
                    for response_key,response_val in response.items():
                        results[response_key] = response_val

            return

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

        return results


    def get_jobs_done(self, node):
        jobs_done = {}

        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('https://', adapter)

        resp = session.get(self._config["report_url"] + "?created-by=" + node)

        data = json.loads(resp.content.decode('utf-8'))

        try:
            if data["meta"]["total"] > 0:
                for i in data["reports"]:
                    jobs_done[i["report-header"]["reporting-period"]["begin-date"]] \
                        = i["id"]
        except:
            return None

        return jobs_done


if __name__ == "__main__":
  md = MetricsReporter()
  # md.get_report_header("01/20/2018", "02/20/2018")
  # md.get_report_datasets("05/01/2018", "05/31/2018")
  # md.query_solr("df35b.302.1")
  # md.report_handler("05/01/2018", "05/30/2018")
  # md.get_unique_pids("05/01/2018", "05/31/2018", "urn:node:KNB")
  md.scheduler()
