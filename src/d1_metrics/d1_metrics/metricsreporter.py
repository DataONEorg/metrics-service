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
import xmltodict
from datetime import datetime
from d1_metrics.metricselasticsearch import MetricsElasticSearch

DEFAULT_REPORT_CONFIGURATION={
    "report_url" : "https://metrics.test.datacite.org/reports",
    "auth_token" : "",
    "report_name" : "Dataset Master Report",
    "release" : "RD1",
    "created_by" : "DataONE",
    "solr_query_url": "https://cn.dataone.org/cn/v2/query/solr/?"
}


class MetricsReporter(object):

    def __init__(self):
        self._config = DEFAULT_REPORT_CONFIGURATION

    def generate_reports(self, begin_date, end_date):
        json_object = {}
        json_object["report-header"] = self.get_report_header(begin_date, end_date)
        json_object["report-datasets"] = self.get_report_datasets()
        pass


    def get_report_header(self, begin_date, end_date):
        report_header = {}
        report_header["report-name"] = self._config["report_name"]
        report_header["report-id"] = "DSR-" + datetime.today().strftime('%Y-%m-%d-%H-%M')
        report_header["release"] = self._config["release"]
        report_header["report-filters"] = [
			  {
				"Name": "Begin-Date",
				"Value": (datetime.strptime(begin_date,'%m/%d/%Y')).strftime('%Y-%m-%d')
			  },
			  {
				"Name": "End-Date",
				"Value": (datetime.strptime(end_date,'%m/%d/%Y')).strftime('%Y-%m-%d')
			  }
		]
        report_header["created"] = datetime.today().strftime('%Y-%m-%d')
        report_header["created-by"] = self._config["created_by"]
        print(report_header)


    def get_report_datasets(self, begin_date, end_date ):
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        report_datasets = []
        fields = ["pid", "nodeId", "inFullRobotList", "formatType", "sessionId"]
        data, total_hits = metrics_elastic_search.getSearches(limit=10,fields=fields, date_start = datetime.strptime(begin_date,'%m/%d/%Y'), date_end = datetime.strptime(end_date,'%m/%d/%Y'))
        total_events = []
        unique_events = []
        for i in data:
            events = []
            events.append(i["pid"])
            events.append(i["nodeId"])
            events.append(i["inFullRobotList"])
            events.append(i["formatType"])
            # events.append(i["sessionId"])
            total_events.append(events)
            if events not in unique_events:
                unique_events.append(events)





        for i in unique_events:
            dataset_object = {}
            dataset_object["dataset-title"]
            dataset_object["publisher"]
            dataset_object["publisher-id"]
            dataset_object["platform"]
            dataset_object["dataset-contributors"]
            dataset_object["dataset-dates"]
            dataset_object["uri"]
            dataset_object["yop"]
            dataset_object["data-type"]
            period_object = {
                "end-date": end_date.strftime('%Y-%m-%d'),
                "begin-date": end_date.strftime('%Y-%m-%d')
            }
            dataset_object["performance"]
            performance_object = {}
            performance_object["period"] = period_object





    def send_reports(self):
        '''
        Sending the reports to the Hub
        Prints out the HTTP Success / Error Response from the Hub.
        :return: void
        '''
        s = requests.session()
        s.headers.update(
            {'Authorization': f'Bearer {self._config["auth_token"]}', 'Content-Type': 'application/json', 'Accept': 'application/json'})
        with open('metricsReport.json', 'r') as content_file:
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
        :return: Ordered dictionary containing the metadata fields queried from Solr
        '''
        queryString = 'q=id:"' + PID +  '"&fl=origin,pubDate,title'

        # connection = urllib.request.urlopen(self.URL+queryString)
        # response = eval(connection.read())
        # return response

        # print(self.URL+queryString)

        response = requests.get(url = self._config["solr_query_url"], params = queryString)
        #The response is returned in XML format.

        orderedDict = xmltodict.parse(response.content)
        return orderedDict


if __name__ == "__main__":
  md = MetricsReporter()
  md.get_report_header("01/20/2018", "02/20/2018")
  # md.get_report_datasets("01/01/2018", "05/31/2018")