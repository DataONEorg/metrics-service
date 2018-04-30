#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import sys

import requests
import xmltodict
from elasticsearch5 import Elasticsearch
from elasticsearch5 import helpers


class ReportHandler:
    """

    """
    def sendReports(self):
        '''
        Sending the reports to the Hub
        Prints out the HTTP Success / Error Response from the Hub.
        :return: void
        '''
        url = 'https://metrics.test.datacite.org/reports'
        token = ''
        s = requests.session()
        s.headers.update(
            {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json', 'Accept': 'application/json'})
        with open('metricsReport.json', 'r') as content_file:
            content = content_file.read()
        r = s.post(url, data=content.encode("utf-8"))
        print('Sending report to the hub')

        print('')
        print(r.status_code, r.reason)
        print('')
        print("Headers: " + str(r.headers))
        print('')
        print("Content: " + str(r.content))

    def getSolrResults(self, PID):
        '''
        Queries the Solr end-point for metadata given the PID.
        :param PID:
        :return: Ordered dictionary containing the metadata fields queried from Solr
        '''
        queryString = 'q=id:"' + PID + '"&fl=origin,pubDate,title'

        # connection = urllib.request.urlopen(self.URL+queryString)
        # response = eval(connection.read())
        # return response

        # print(self.URL+queryString)

        response = requests.get(url=self.URL, params=queryString)
        # The response is returned in XML format.

        orderedDict = xmltodict.parse(response.content)
        return orderedDict
