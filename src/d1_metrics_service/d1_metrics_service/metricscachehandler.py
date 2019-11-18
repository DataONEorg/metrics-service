"""
Metrics Cache Handler module

"""

import ast
import asyncio
import concurrent.futures
import json
import logging
import schedule
import time
from collections import OrderedDict
from datetime import datetime, timedelta
from urllib.parse import quote_plus, unquote, urlparse

import requests

import falcon
from aiohttp import ClientSession
from d1_metrics.metricsdatabase import MetricsDatabase
from d1_metrics.metricselasticsearch import MetricsElasticSearch
from d1_metrics_service import pid_resolution
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


### Configurations section

DEFAULT_CACHE_CONFIGURATION={
    "cn_solr_url": "https://cn.dataone.org/cn/v2/query/solr/",
    "cn_stage_solr_url": "https://cn-stage.test.dataone.org/cn/v2/query/solr/",
    "dev_solr_url": "https://dev.nceas.ucsb.edu/knb/d1/mn/v2/query/solr/",
    "caching_end_point": "https://handy-owl.nceas.ucsb.edu/apacheTest/metrics",
}

# List of characters that should be escaped in solr query terms
SOLR_RESERVED_CHAR_LIST = [
  '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':'
  ]

CONCURRENT_REQUESTS = 5  #max number of concurrent requests to run


class MetricsCacheHandler:
    """
    Metrics Cahce Handler class manages and updates the Apache Caching files
    for the metrics service
    """

    def __init__(self):
        """
        Default init
        """
        self._config = DEFAULT_CACHE_CONFIGURATION
        logging.basicConfig()
        self.logger = logging.getLogger('metrics_service.' + __name__)
        self.logger.setLevel(logging.DEBUG)


    def get_portal_labels(self, solr_url = None, ):
        """
            Queries the SOLR endpoint to get the list of portals.
            :params: solr_url
            :returns:
                Dictionary object of Portal Labels
        """
        self.logger.info("Retrieving all the Portal Labels from SOLR")

        portal_label_dict = {}
        if solr_url == None:
            url = self._config["dev_solr_url"]
        session = requests.Session()
        params = {
                'wt':'json',
                'fl':'id, label, collectionQuery',
                'q.op':'OR',
                'rows':'10000'
            }
        
        params['q'] = "formatId:\"https://purl.dataone.org/portals-1.0.0\" AND -obsoletedBy:*"
        response = session.get(url, params=params)

        response_text = response.text
        if response.status_code == 200:
            json_response = response.json()
            self.logger.info("Total non-obsoleted labels found : %d", json_response["response"]["numFound"])
            for response_object in json_response["response"]["docs"]:
                if response_object["id"] not in portal_label_dict:
                    portal_label_dict[response_object["id"]] = {}
                    portal_label_dict[response_object["id"]]["label"] = response_object["label"]
                    portal_label_dict[response_object["id"]]["collectionQuery"] = response_object["collectionQuery"]
        else:
            self.logger.error("Request got error code")

        return portal_label_dict


if __name__ == "__main__":
    cache_handler = MetricsCacheHandler()
    # cache_handler.perform_cache_refresh()
    cache_handler.cache_update_job()
    # cache_handler.get_portal_labels()


