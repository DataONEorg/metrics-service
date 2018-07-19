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
    "report_url" : "https://metrics.test.datacite.org/reports",
    "auth_token" : "",
    "report_name" : "Dataset Master Report",
    "report_id" : datetime.today().strftime('%Y-%m-%d'),
    "release" : "RD1",
    "created_by" : "DataONE",
    "solr_query_url": "https://cn.dataone.org/cn/v2/query/solr/?"
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
        print("handling report for", start_date, end_date)
        json_object = {}
        json_object["report-header"] = self.get_report_header(start_date, end_date)
        json_object["report-datasets"] = self.get_report_datasets(start_date, end_date)
        with open('./reports/' + (datetime.strptime(end_date,'%m/%d/%Y').strftime('%Y-%m-%d'))+'.json', 'w') as outfile:
            print("Writing to file")
            json.dump(json_object, outfile, indent=2,ensure_ascii=False)
        # self.send_reports()
        return


    def get_report_header(self, start_date, end_date):
        """
        Generates a unique report header
        :param start_date:
        :param end_date:
        :return: Dictionary report header object.
        """
        report_header = {}
        report_header["report-name"] = self._config["report_name"]
        report_header["report-id"] = "DSR-" + datetime.today().strftime('%Y-%m-%d-%H-%M')
        report_header["release"] = self._config["release"]
        report_header["reporting-period"] = [
			  {
				"begin-date" : (datetime.strptime(start_date,'%m/%d/%Y')).strftime('%Y-%m-%d')
			  },
			  {
				"end-date" : (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d')
			  }
		]
        report_header["created"] = self._config["report_id"]
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
                "term": {"inFullRobotList": "false"}
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

        print("Unique pids for ", start_date, end_date, len(unique_pids))

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
                "term": {"event.key": "read"}
            },
            {
                "term": {"inFullRobotList": "false"}
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
            print(count, " of " , len(unique_pids))

            dataset = {}
            solr_response = self.query_solr(pid)
            if(solr_response["response"]["numFound"] > 0):

                if ("title" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["dataset-title"] = solr_response["response"]["docs"][0]["title"]

                if ("authoritativeMN" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["publisher"] = self.resolve_MN(solr_response["response"]["docs"][0]["authoritativeMN"])

                if ("authoritativeMN" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["publisher-id"] = {"type":"https://cn.dataone.org/cn/v2/node/", "value" :solr_response["response"]["docs"][0]["authoritativeMN"]}

                dataset["platform"] = "DataONE"

                if ("origin" in (i for i in solr_response["response"]["docs"][0])):
                    contributors = []
                    for i in solr_response["response"]["docs"][0]["origin"]:
                        contributors.append({"type": "Name", "value": i})
                    dataset["contributors"] = contributors

                if ("datePublished" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["dataset-dates"] = {"type": "pub-date", "value" :solr_response["response"]["docs"][0]["datePublished"][:10]}
                else:
                    dataset["dataset-dates"] = {"type": "pub-date", "value" :solr_response["response"]["docs"][0]["dateUploaded"][:10]}

                if "doi" in pid:
                    dataset["dataset-id"] = [{"DOI": pid}]
                else:
                    dataset["dataset-id"] = [{"other-id": pid}]

                dataset["yop"] = dataset["dataset-dates"]["value"][:4]

                if ("dataUrl" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["uri"] = solr_response["response"]["docs"][0]["dataUrl"]

                dataset["data-type"] = "Dataset"

                dataset["access-type"] = "regular"

                dataset["performance"] = {}

                dataset["performance"]["reporting-period"] = [
                      {
                        "begin-date" : (datetime.strptime(start_date,'%m/%d/%Y')).strftime('%Y-%m-%d')
                      },
                      {
                        "end-date" : (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d')
                      }
                ]

                instance = []
                pid_list = []
                pid_list.append(pid)
                pid_list = self.resolvePIDs(pid_list)

                print(json.dumps(pid_list, indent=2))


                for i in pid_list:
                    if i in unique_pids:
                        unique_pids.remove(i)

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

                dataset["performance"]["instance"] = instance



            else:
                continue

            report_datasets.append(dataset)
            # print(json.dumps(dataset, indent=2))

            if (count == 10):
                break

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

            # Forming the query string and url encoding the identifier to escape special chartacters
            queryString = 'fq=id:' + quote_plus(identifier) + '&fl=documents,obsoletes,resourceMap&wt=json'
            print(queryString)

            # Getting length of the array from previous iteration to control the loop
            prevLength = len(PIDs)

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
            if (prevLength == len(PIDs)):
                callSolr = False

        return PIDs


    def send_reports(self):
        """
        Sends report to the Hub at the specified Hub report url in the config parameters
        :return: Nothing
        """
        s = requests.session()
        s.headers.update(
            {'Authorization': f'Bearer {self._config["auth_token"]}', 'Content-Type': 'application/json', 'Accept': 'application/json'})
        with open(self._config["report_id"]+'.json', 'r') as content_file:
            content = content_file.read()
        r = s.post(self._config["report_url"], data=content.encode("utf-8"))
        print('Sending report to the hub')

        print('')
        print(r.status_code, r.reason)
        print('')
        print("Headers: " + str(r.headers))
        print('')
        print("Content: " + str(r.content))


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
        date = datetime(2018, 5, 15)
        count = 0
        while (date != datetime.today().strftime('%Y-%m-%d')):
            count = count + 1
            if(count == 10):
                break
            prevDate = date
            date += timedelta(days=1)

            print("Job ", count, " : ", prevDate.strftime('%m/%d/%Y'), " to ", date.strftime('%m/%d/%Y'))

            # Uncomment me to send reports to the HUB!
            self.report_handler(prevDate.strftime('%m/%d/%Y'), date.strftime('%m/%d/%Y'))

            print("Job ", count, " : ", prevDate, " to ", date)


if __name__ == "__main__":
  md = MetricsReporter()
  # md.get_report_header("01/20/2018", "02/20/2018")
  # md.get_report_datasets("05/01/2018", "05/31/2018")
  # md.resolve_MN("urn:node:KNB")
  # md.query_solr("df35b.302.1")
  # md.report_handler("05/19/2018", "05/20/2018")
  # md.get_unique_pids("05/01/2018", "05/31/2018")
  md.scheduler()