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


    def report_handler(self, start_date, end_date):
        """
        Creates a Report JSON object, dumps it to a file and sends the report to the Hub.
        This is a handler function that manages the entire work flow
        :param start_date:
        :param end_date:
        :return: None
        """
        json_object = {}
        json_object["id"] = "DSR-D1-" + (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d')
        json_object["report-header"] = self.get_report_header(start_date, end_date)
        json_object["report-datasets"] = self.get_report_datasets(start_date, end_date)
        with open('./reports/' + ("DSR-D1-" + (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d'))+'.json', 'w') as outfile:
            json.dump(json_object, outfile, indent=2,ensure_ascii=False)
        response = self.send_reports(start_date, end_date)
        return response


    def get_report_header(self, start_date, end_date):
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
        report_header["created-by"] = self._config["created_by"]
        report_header["report-filters"] = []
        report_header["report-attributes"] = []

        return (report_header)


    def get_unique_pids(self, start_date, end_date):
        """
        Queries ES for the given time period and returns the set of pids
        :param start_date:
        :param end_date:
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
                "exists": {
                    "field": "sessionId"
                }
            }
        ]
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
                    if i["key"]["country"] in report_instances["METADATA"]["country_unique_investigations"]:
                        report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] = \
                            report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] + 1
                        report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] = \
                        report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] + i["doc_count"]
                    else:
                        report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] = 1
                        report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] = i["doc_count"]
                else:
                    report_instances["METADATA"] = {
                        "unique_investigations" : 1,
                        "total_investigations": i["doc_count"],
                        "country_unique_investigations": {
                            i["key"]["country"]: 1
                        },
                        "country_total_investigations": {
                            i["key"]["country"]: i["doc_count"]
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
                    if i["key"]["country"] in report_instances["DATA"]["country_unique_requests"]:
                        report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] = \
                            report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] + 1
                        report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] = \
                            report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] + i[
                                "doc_count"]
                        report_instances["DATA"]["country_unique_requests"][i["key"]["country"]] = \
                            report_instances["DATA"]["country_unique_requests"][i["key"]["country"]] + 1
                        report_instances["DATA"]["country_total_requests"][i["key"]["country"]] = \
                            report_instances["DATA"]["country_total_requests"][i["key"]["country"]] + i[
                                "doc_count"]
                    else:
                        if i["key"]["country"] in report_instances["METADATA"]["country_unique_investigations"]:
                            report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] = \
                                report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] + 1
                            report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] = \
                                report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] + i[
                                    "doc_count"]
                        else:
                            report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] = 1
                            report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] = i[
                                "doc_count"]
                        report_instances["DATA"]["country_unique_requests"][i["key"]["country"]] = 1
                        report_instances["DATA"]["country_total_requests"][i["key"]["country"]] = i[
                            "doc_count"]
                else:
                    if "METADATA" in report_instances:
                        report_instances["METADATA"]["unique_investigations"] = report_instances["METADATA"][
                                                                                    "unique_investigations"] + 1
                        report_instances["METADATA"]["total_investigations"] = report_instances["METADATA"][
                                                                                   "total_investigations"] + i[
                                                                                   "doc_count"]
                        if i["key"]["country"] in report_instances["METADATA"]["country_unique_investigations"]:
                            report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] = \
                                report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] + 1
                            report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] = \
                                report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] + i[
                                    "doc_count"]
                        else:
                            report_instances["METADATA"]["country_unique_investigations"][i["key"]["country"]] = 1
                            report_instances["METADATA"]["country_total_investigations"][i["key"]["country"]] = i[
                                "doc_count"]
                    else:
                        report_instances["METADATA"] = {
                            "unique_investigations": 1,
                            "total_investigations": i["doc_count"],
                            "country_unique_investigations": {
                                i["key"]["country"]: 1
                            },
                            "country_total_investigations": {
                                i["key"]["country"]: i["doc_count"]
                            }
                        }
                    report_instances["DATA"] = {
                        "unique_requests": 1,
                        "total_requests": i["doc_count"],
                        "country_unique_requests": {
                            i["key"]["country"]: 1
                        },
                        "country_total_requests": {
                            i["key"]["country"]: i["doc_count"]
                        }
                    }
        return report_instances



    def get_report_datasets(self, start_date, end_date ):
        """

        :param start_date:
        :param end_date:
        :return:
        """
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        report_datasets = []
        pid_list = []

        unique_pids = self.get_unique_pids(start_date, end_date)
        count = 0
        for pid in unique_pids:
            count = count + 1
            if((count % 100 == 0) or (count == 1) or (count == len(unique_pids))) :
                print(count, " of " , len(unique_pids))


            dataset = {}
            solr_response = self.query_solr(pid)
            if(solr_response["response"]["numFound"] > 0):

                if ("title" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["dataset-title"] = solr_response["response"]["docs"][0]["title"]

                if ("authoritativeMN" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["publisher"] = self.resolve_MN(solr_response["response"]["docs"][0]["authoritativeMN"])

                if ("authoritativeMN" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["publisher-id"] = []
                    dataset["publisher-id"].append({"type":"grid", "value" :solr_response["response"]["docs"][0]["authoritativeMN"]})

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

                dataset["access-method"] = "regular"

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
                                                   "metric-type": "total-dataset-investigations",
                                                   "country-counts": report_instances["METADATA"]["country_total_investigations"]}

                    unique_dataset_investigation = {"count": report_instances["METADATA"]["unique_investigations"],
                                                    "metric-type": "unique-dataset-investigations",
                                                    "country-counts": report_instances["METADATA"]["country_unique_investigations"]}
                    instance.append(total_dataset_investigation)
                    instance.append(unique_dataset_investigation)


                if("DATA" in report_instances):
                    total_dataset_requests = {"count": report_instances["DATA"]["total_requests"],
                                                   "metric-type": "total-dataset-requests",
                                                   "country-counts": report_instances["DATA"]["country_total_requests"]}

                    unique_dataset_requests = {"count": report_instances["DATA"]["unique_requests"],
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


    def send_reports(self,  start_date, end_date):
        """
        Sends report to the Hub at the specified Hub report url in the config parameters
        :return: Nothing
        """
        s = requests.session()
        s.headers.update(
            {'Authorization': "Bearer " +  self._config["auth_token"], 'Content-Type': 'application/json', 'Accept': 'application/json'})
        with open("./reports/DSR-D1-" + (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d')+'.json', 'r') as content_file:
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
        date = datetime(2012, 7, 5)
        stopDate = datetime(2012, 12, 31)

        count = 0
        while (date.strftime('%Y-%m-%d') != stopDate.strftime('%Y-%m-%d')):
            print(date.strftime('%Y-%m-%d'))
            print(stopDate.strftime('%Y-%m-%d'))
            count  = count + 1

            prevDate = date
            date += timedelta(days=1)

            print("Job ", count, " : ", prevDate.strftime('%m/%d/%Y'), " to ", date.strftime('%m/%d/%Y'))

            # Uncomment me to send reports to the HUB!
            response = self.report_handler(prevDate.strftime('%m/%d/%Y'), date.strftime('%m/%d/%Y'))


            with open('./reports/reports.log', 'a') as logfile:
                logentry = "Job "+ str(count) + " : " + prevDate.strftime('%m/%d/%Y') + " to " + date.strftime('%m/%d/%Y') + " === " + str(response.status_code)
                logfile.write(logentry)
                logfile.write("\n")

            print("Job ", count, " : ", prevDate, " to ", date)

            if response.status_code != 201:
                with open('./reports/reports_errors.log', 'a') as errorfile:
                    logentry = "Job " + str(count) + " : " + prevDate.strftime('%m/%d/%Y') + " to " + date.strftime(
                        '%m/%d/%Y') + " === " + str(response.status_code)
                    errorfile.write("\n")
                    errorfile.write(datetime.now().strftime("%I:%M%p on %B %d, %Y"))
                    errorfile.write("\n")
                    errorfile.write(logentry)
                    errorfile.write("\n")
                    errorfile.write(str(response.status_code) + " " + response.reason)
                    errorfile.write("\n")
                    errorfile.write("Headers: ")
                    errorfile.write(str(response.headers))
                    errorfile.write("\n")
                    errorfile.write("Content: ")
                    errorfile.write(str(response.content))
                    errorfile.write("\n")




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