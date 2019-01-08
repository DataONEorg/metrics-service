'''
Implements a wrapper for the metrics reporting service.
'''

import argparse
import sys
import requests
import json
import urllib.request
from xml.etree import ElementTree
import logging


DEFAULT_REPORT_CONFIGURATION={
    "report_url" : "https://api.datacite.org/reports/",
    "solr_query_url": "https://cn.dataone.org/cn/v2/query/solr/"
}


class MetricsReportUtilities(object):

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

    def get_created_reports(self):
        """
        Gets the reports that are created by DataONE to the DataCite HUB
        Returns the results at the MN level
        :return:
        """
        mn_list = self.get_MN_List()
        return_dict = {}
        total_sum = 0
        for i in mn_list:
            s = requests.session()

            response = s.get(self._config["report_url"] + '?created_by=' + i)

            data = json.loads(response.text)

            total_sum += data["meta"]["total"]
            return_dict.update(self.parse_json_response(data))

        return  return_dict

    def parse_json_response(self, data):
        """
        Parses the response data
        :param data: Response data
        :return:
        """
        sent_dict = {}
        for i in data["reports"]:
            try:
                id = i["id"]
                created_by = i["report-header"]["created-by"]
                created = i["report-header"]["created"]
                start_date = i["report-header"]["reporting-period"]["begin-date"]
                end_date = i["report-header"]["reporting-period"]["end-date"]
                if created_by not in sent_dict:
                    sent_dict[created_by] = []
                sent_dict[created_by].append(start_date)
            except KeyError as e:
                pass

        return sent_dict


if __name__ == "__main__":
  md = MetricsReportUtilities()
  print(md.get_created_reports())
