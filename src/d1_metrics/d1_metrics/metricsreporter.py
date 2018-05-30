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

    def generate_reports(self):
        metrics_elastic_Search = MetricsElasticSearch()





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


