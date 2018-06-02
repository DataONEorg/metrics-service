'''
Implements a wrapper for the metrics reporting service.
'''

from elasticsearch5 import Elasticsearch
from elasticsearch5 import helpers

import argparse
import sys
import requests
import json
import urllib.request
from xml.etree import ElementTree
from datetime import datetime
from d1_metrics.metricselasticsearch import MetricsElasticSearch

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
        json_object = {}
        json_object["report-header"] = self.get_report_header(start_date, end_date)
        json_object["report-datasets"] = self.get_report_datasets(start_date, end_date)
        with open((datetime.strptime(end_date,'%m/%d/%Y').strftime('%Y-%m-%d'))+'.json', 'w') as outfile:
            json.dump(json_object, outfile, indent=2,ensure_ascii=False)
        # self.send_reports()


    def get_report_header(self, start_date, end_date):
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
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        pid_list = []
        unique_pids = ()
        query = [
            {
                "term": {"formatType": "METADATA"}
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
        unique_pids = set(pid_list)
        return unique_pids



    def generate_instances(self, start_date, end_date):
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        report_instances = {}
        data = metrics_elastic_search.iterate_composite_aggregations(start_date=datetime.strptime(start_date,'%m/%d/%Y'),\
                                                                     end_date=datetime.strptime(end_date,'%m/%d/%Y'))
        for i in data["aggregations"]["pid_list"]["buckets"]:
            if i["key"]["pid"] in report_instances:
                prev_total_data_investigations = report_instances[i["key"]["pid"]]["total_data_investigations"]
                prev_unique_data_investigations = report_instances[i["key"]["pid"]]["unique_data_investigations"]
                if i["key"]["country"] in report_instances[i["key"]["pid"]]["country_total_investigations"]:
                    prev_country_total_investigations = report_instances[i["key"]["pid"]]["country_total_investigations"][i["key"]["country"]]
                    prev_country_unique_investigations = \
                    report_instances[i["key"]["pid"]]["country_unique_investigations"][i["key"]["country"]]
                else:
                    prev_country_total_investigations = 0
                    prev_country_unique_investigations = 0
                report_instances[i["key"]["pid"]] = {
                    "total_data_investigations": prev_total_data_investigations + i["doc_count"],
                    "unique_data_investigations": prev_unique_data_investigations + 1,
                    "country_total_investigations": {
                        i["key"]["country"]: prev_country_total_investigations + i["doc_count"]
                    },
                    "country_unique_investigations": {
                        i["key"]["country"]: prev_country_unique_investigations + 1
                    }
                }
            else:
                report_instances[i["key"]["pid"]] = {
                    "total_data_investigations" : i["doc_count"],
                    "unique_data_investigations": 1,
                    "country_total_investigations": {
                        i["key"]["country"]: i["doc_count"]
                    },
                    "country_unique_investigations" : {
                        i["key"]["country"] : 1
                    }
                }
        # print(json.dumps(report_instances, indent=2))
        return report_instances



    def get_report_datasets(self, start_date, end_date ):
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        report_datasets = []

        unique_pids = self.get_unique_pids(start_date, end_date)
        report_instances = self.generate_instances(start_date, end_date)
        count = 0
        for pid in unique_pids:
            dataset = {}
            solr_response = self.query_solr(pid)
            if(solr_response["response"]["numFound"] > 0):

                if ("dataset-title" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["dataset-title"] = solr_response["response"]["docs"][0]["title"]

                if ("authoritativeMN" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["publisher"] = {"type":"https://cn.dataone.org/cn/v2/node/", "value" :self.resolve_MN(solr_response["response"]["docs"][0]["authoritativeMN"])}

                if ("authoritativeMN" in (i for i in solr_response["response"]["docs"][0])):
                    dataset["publisher-id"] = solr_response["response"]["docs"][0]["authoritativeMN"]

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

                if pid in report_instances:
                    total_dataset_investigation = {"count": report_instances[pid]["total_data_investigations"],
                                                   "metric-type": "total-dataset-investigations",
                                                   "country-counts": report_instances[pid][
                                                       "country_total_investigations"]}

                    unique_dataset_investigation = {"count": report_instances[pid]["unique_data_investigations"],
                                                    "metric-type": "unique-dataset-investigations",
                                                    "country-counts": report_instances[pid][
                                                        "country_unique_investigations"]}

                    instance.append(total_dataset_investigation)
                    instance.append(unique_dataset_investigation)

                    dataset["performance"]["instance"] = instance
                else:
                    count = count + 1
                    print(count, " : ", pid)
                    print()



            report_datasets.append(dataset)
        return (report_datasets)


    def resolve_MN(self, authoritativeMN):
        node_url = "https://cn.dataone.org/cn/v2/node/" + authoritativeMN
        resp = requests.get(node_url, stream=True)
        root = ElementTree.fromstring(resp.content)
        name = root.find('name').text
        return name



    def send_reports(self):
        '''
        Sending the reports to the Hub
        Prints out the HTTP Success / Error Response from the Hub.
        :return: void
        '''
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
        '''
        Queries the Solr end-point for metadata given the PID.
        :param PID:
        :return: JSON Object containing the metadata fields queried from Solr
        '''
        queryString = 'q=id:"' + PID + '"&fl=origin,title,datePublished,dateUploaded,authoritativeMN,dataUrl&wt=json'
        response = requests.get(url = self._config["solr_query_url"], params = queryString)

        return response.json()


if __name__ == "__main__":
  md = MetricsReporter()
  # md.get_report_header("01/20/2018", "02/20/2018")
  # md.get_report_datasets("05/01/2018", "05/31/2018")
  # md.resolve_MN("urn:node:KNB")
  # md.query_solr("df35b.302.1")
  md.report_handler("05/01/2018", "05/31/2018")
  # md.get_unique_pids()