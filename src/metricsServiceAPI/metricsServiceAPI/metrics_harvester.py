#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import sys

import requests
import xmltodict
from elasticsearch5 import Elasticsearch
from elasticsearch5 import helpers


class MetricsHarvester:
    ES = Elasticsearch()

    def __init__(self, index_name):
        self.index_name = index_name

    def getMetricsInfo(self, indexname, sessionid=None):
        '''
        set up the query. we're using the scan helper here,
        so for performance reasons, we don't bother sorting --
        it would require setting preserve_order on the scan,
        and the docs describe that as 'extremely expensive'
        :param indexname:
        :param sessionid:
        :return: array with the download information.
        '''
        searchbody = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {"_type": "logevent"}
                        },
                        {
                            "term": {"beat.name": "eventlog"}
                        },
                        {
                            "term": {"formatType": "data"}
                        },
                        # {
                        #     "term": {"event": "read"}
                        # },
                        {
                            "exists": {"field": "sessionid"}
                        }
                        # {
                        #     "range": {
                        #         "dateLogged": {
                        #             "gte": "2017-10-01T00:00:00Z",
                        #             "lt": "2017-12-01T00:00:00Z"
                        #         }
                        #     }
                        # }
                    ]
                }
            }
        }

        # add a sessionid section to the 'must' if requested
        if sessionid is not None:
            sessionid_search = {"term": {"sessionid": sessionid}}
            searchbody["query"]["bool"]["must"].append(sessionid_search)

        results = helpers.scan(self.ES, query=searchbody)

        download_info = []
        event_count = 0
        count = 0

        # ?]f = open("pids.txt", "w+")

        for event in results:
            # f.write(event["_source"]["pid"] + "\n")
            # qS = querySolr(event["_source"]["pid"])
            # orderedDict = qS.getSolrResults()
            event_count += 1
            # try:
            #     if (orderedDict["response"]["result"]["doc"] is None):
            #         count += 1
            #     else:
            #         print(orderedDict["response"]["result"]["doc"])
            # except:
            #     print(event["_source"]["pid"] + "generated error!")
            download = {}
            # download["Title"] = orderedDict["response"]["result"]["doc"]["str"]["#text"]
            # download["Publisher"] = "DataOne"
            # download["Publisher_ID"] = ""
            # download["Creators"] = orderedDict["response"]["result"]["doc"]["arr"]["str"][:]
            # download["PublicationDate"] = orderedDict["response"]["result"]["doc"]["date"]["#text"]
            # download["Dataset_Version"] = event["_source"]["pid"]
            # download["DOI"] = event["_source"]["pid"]
            # download["Other_ID"] = event["_source"]["pid"]
            # download["URI"] = ""
            # download["YOP"] = ""
            # download["Access_Method"] = ""
            # download["Metric_Type"] = ""
            # download["Reporting_Period_Total"] = ""
            # download["mmm-yyyy"] = ""
            download["timestamp"] = event["_source"]["@timestamp"]
            download["event"] = event["_source"]["event"]
            # download["date"] = event["_source"]["dateLogged"]
            # download["formatId"] = event["_source"]["formatId"]
            # download["formatType"] = event["_source"]["formatType"]
            download["pid"] = event["_source"]["pid"]
            # download["sessionId"] = event["_source"]["sessionid"]
            download["location"] = event["_source"]["location"]
            # download["host"] = event["_source"]["host"]
            # download["nodeId"] = event["_source"]["nodeId"]
            # download["size"] = event["_source"]["size"]
            # download["useragent"] = event["_source"]["userAgent"]
            download["count"] = event_count
            download_info.append(download)
        return download_info
        # print(count + "objects not found out of " + event_count)
        # f.close()


if __name__ == "__main__":
    metrics_harvester = MetricsHarvester('logstash-test0')

    metrics_harvester.ES.indices.refresh(metrics_harvester.index_name)

    download_info = metrics_harvester.getMetricsInfo(metrics_harvester.index_name)

    print(json.dumps(download_info, indent=2))
    exit(0)
