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
    "caching_end_point": "https://logproc-stage-ucsb-1.test.dataone.org/test/metrics",
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
        logging.basicConfig(filename='/var/log/metrics-service/cache.log',
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.INFO)
        self.logger = logging.getLogger('metrics_cache_handler')


    def cache_update_job(self):
        """
            Performs Asynchronous Cache update job. Hits the Metrics-Service
            end-point so Apache can handle the 
        """
        t_0 = time.time()
        logger = self.logger
        

        def _fetch(url, portal_label):
            time_fetch_init = time.time()
            session = requests.Session()
            retry = Retry(connect=3, backoff_factor=0.5)
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('https://', adapter)

            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'}

            # Defining the request object
            metrics_request = OrderedDict()
            
            filterBy_label = OrderedDict()
            filterBy_label["filterType"] = "portal"
            filterBy_label["values"] = [portal_label]
            filterBy_label["interpretAs"] = "list"

            filterBy_time = OrderedDict()
            filterBy_time["filterType"] = "month"
            filterBy_time["values"] = [
                            "01/01/2012",
                            datetime.today().strftime('%m/%d/%Y')
                        ]
            filterBy_time["interpretAs"] = "range"
            
            metricsPage = OrderedDict()
            metricsPage["total"] = 0
            metricsPage["start"] = 0
            metricsPage["count"] = 0

            metrics_request["metricsPage"] = metricsPage
            metrics_request["metrics"] = [
                    "citations",
                    "downloads",
                    "views"
                ]
            metrics_request["filterBy"] = [
                filterBy_label,
                filterBy_time
                ]
            metrics_request["groupBy"] = [
                    "month"
                ]

            # Formatting the query
            metrics_request_str = json.dumps(metrics_request)
            query = (metrics_request_str.replace(" ", "")).replace('"','%22')
            metrics_query_url = url + "?metricsRequest=" + query

            logger.debug("Refreshing Cache for the label " + portal_label)
            logger.debug(metrics_query_url)
            response = session.get(metrics_query_url, headers=headers)
            
            logger.info("Cache refreshed for label %s in:  %fsec" , portal_label, time.time() - time_fetch_init)
            logging.info('\n')
            return [portal_label, response.status_code]


        async def _work(portal_labels_dict):
            """
                The async work function utilizes the private method _fetch to handle concurrent calls
                to refresh the cache.

                Args:
                    portal_labels_dict: dictionary object retrieved from solr

                Returns:
                    None
            """
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
                loop = asyncio.get_event_loop()
                tasks = []

                # Create an individual cache refresh task for every label retrieved from solr
                for portal_pid in portal_labels_dict:
                    label = portal_labels_dict[portal_pid]["label"]
                    url = self._config["caching_end_point"]
                    tasks.append(loop.run_in_executor(executor, _fetch, url, label )) 
 
                # wait for the response from the server
                for response in await asyncio.gather(*tasks):
                    results[ response[0] ] = response[1]

                    # handle the case when the response is not 200. report.
                    if response[1] != 200:
                        self.logger.error("Received an error for " + response[0] + " with response code " + str(response[1]))

        results = {}
        self.logger.info("Begining the caching process")
        
        # In a multithreading environment such as under gunicorn, the new thread created by
        # gevent may not provide an event loop. Create a new one if necessary.
        
        try:
        
            loop = asyncio.get_event_loop()
        
        except RuntimeError as e:
        
            self.logger.info("Creating new event loop.")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        portal_labels_dict = self.get_portal_labels()
        future = asyncio.ensure_future(_work(portal_labels_dict))

        loop.run_until_complete( future )
        self.logger.info("Total Time Elapsed :%fsec", time.time()-t_0)

        return None


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


    def perform_cache_refresh(self):
        """
            Cache Refresh Job scheduler
            Schedules a cache refresh job every day at 12:30 

            :param: None
            :returns: None
        """
        
        schedule.every().day.at("00:30").do(self.cache_update_job())

        while True:
            self.logger.info("Performing Cache Refresh")
            schedule.run_pending()
            time.sleep(60)


if __name__ == "__main__":
    cache_handler = MetricsCacheHandler()
    cache_handler.perform_cache_refresh()
    # cache_handler.cache_update_job()


