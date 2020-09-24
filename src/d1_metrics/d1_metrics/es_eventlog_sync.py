"""

Script to keep the ES eventlog index upto date with the aggregated objects like -
DataONE portal objects, DataONE User profiles, etc...

Note: This script needs Python 3.6 or higher.

"""

import os
import re
import sys
import json
import asyncio
import logging
import argparse
import datetime
import requests
import concurrent.futures


from d1_metrics import common
from d1_metrics.solrclient import SolrClient
from d1_metrics.metricselasticsearch import MetricsElasticSearch

from d1_metrics_service import pid_resolution

DATAONE_CN_SOLR = [
    "https://cn.dataone.org/cn/v2/query"
]

DATAONE_MN_SOLR = [
    "https://arcticdata.io/metacat/d1/mn/v2/query",
    "https://knb.ecoinformatics.org/knb/d1/mn/v2/query",
]

CONCURRENT_REQUESTS = 20  # max number of concurrent requests to run

ZERO = datetime.timedelta(0)

BATCH_TDELTA_PERIOD = datetime.timedelta(minutes=10)


# ==========


def getModifiedPortals():
    """
    Queries Solr to get the list of portals that were modified
    :return:
    """
    pass


def querySolr():
    """
    Queries Solr end point
    :return:
    """

    pass


def performRegularPortalChecks():
    """
    Checks for newly added datasets that satisfy the collection query
    associated with this portal
    :return:
    """
    pass


def _doPost(session, url, params, use_mm=True):
    """
    Post a request, using mime-multipart or not. This is necessary because
    calling solr on the local address on the CN bypasses the CN service interface which
    uses mime-multipart requests.

    Args:
    session: Session instance
    url: URL for request
    params: params configure for mime-multipart request
    use_mm: if not true, then a form request is made.

    Returns: response object
    """
    if use_mm:
        return session.post(url, files=params)
    paramsd = {key: value[1] for (key, value) in params.items()}
    # This is necessary because the default query is set by the DataONE SOLR connector
    # which is used when accessing solr through the public interface.
    if not 'q' in paramsd:
        paramsd['q'] = "*:*"
    return session.post(url, data=paramsd)


def getPortalMetadata(url=None, seriesId=""):
    """
    Retrieves portal attributes from solr
    :return:
    """
    if url is None:
        url = "https://cn.dataone.org/cn/v2/query"

    session = requests.Session()

    # Create a solr client object
    solrClientObject = SolrClient(url, "solr")
    portal_metadata = {}

    params = {'wt': (None, 'json'),
              'fl': (None, 'seriesId,collectionQuery'),
              'rows': (None, 1)
              }

    query_string = "(((seriesId:*" + seriesId + "*) OR (seriesId:*" + seriesId.upper() + \
                   "*) OR (seriesId:*" + seriesId.lower() + "*)) AND -obsoletedBy:* AND formatType:METADATA)"

    params['fq'] = (None, query_string)

    try:
        solr_response = _doPost(session, url, params)

        if solr_response["numFound"] > 0:
            portal_metadata["seriesId"] = solr_response["docs"][0]["seriesId"]
            portal_metadata["seriesId"] = solr_response["docs"][0]["seriesId"]
            print(portal_metadata)

    except Exception as e:
        print(e)
    pass



def resolvePortalCollectionQuery():
    """

    :return:
    """
    pass


def retrievePortalDatasetIdentifierFamily():
    """

    :return:
    """
    pass


def generatePortalHash(portalDatasetIdentifierFamily=[]):
    """
    Generates hash for a given Portal from list of identifiers
    :return:
    """
    portalDatasetIdentifierFamily.sort()
    return hash(tuple(portalDatasetIdentifierFamily))


def storePortalHash():
    """
    Stores portal hash in the database
    :return:
    """
    pass


def getPIDRecords():
    """
    Queries ES and retrieves records from the index for a given PID
    :return:
    """
    metrics_elastic_search = MetricsElasticSearch()
    metrics_elastic_search.connect()

    test_fixture = testSetUP()
    # set up the query
    query = {
        "term": {
            "pid.key": test_fixture["PID"]
        }
    }

    return metrics_elastic_search.getRawSearches(index="eventlog-0", q=query)


def testSetUP():
    """
    Temporary fixture to test the functionalities
    :return:
    """
    test_fixture = {}

    # Just a random test pid from eventlog-0 index
    test_fixture["PID"] = "aekos.org.au/collection/sa.gov.au/bdbsa_veg/survey_88.20160201"
    test_fixture["portalIdentifiers"] = {}
    test_fixture["portalIdentifiers"]["DBO"] = "urn:uuid:8cdb22c6-cb33-4553-93ca-acb6f5d53ee4"
    return test_fixture


def updateRecords():
    """
    Updates the record and writes it down to the ES index
    :return:
    """
    metrics_elastic_search = MetricsElasticSearch()
    metrics_elastic_search.connect()
    results_tuple = getPIDRecords()
    test_fixtures = testSetUP()
    total_count = len(results_tuple["hits"]["hits"])

    # print(total_count)
    for read_event_entry in results_tuple["hits"]["hits"]:
        # print(json.dumps(read_event_entry, indent=2))

        portalIdentifierArray = []
        if "portalIdentifier" in read_event_entry["_source"]:
            portalIdentifierArray = read_event_entry["_source"]["portalIdentifier"]

        portalIdentifierArray.append(test_fixtures["portalIdentifiers"]["DBO"])
        read_event_entry["_source"]["portalIdentifier"] = portalIdentifierArray

        # print(json.dumps(read_event_entry, indent=2))
        metrics_elastic_search.updateRecord("eventlog-0", read_event_entry)
        break
    return


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-l",
        "--log_level",
        action="count",
        default=0,
        help="Set logging level, multiples for more detailed.",
    )
    parser.add_argument(
        "-t",
        "--test",
        default=False,
        action="store_true",
        help="Show the starting point and number of records to retrieve but don't download.",
    )
    parser.add_argument(
        "-E", "--enddate", default=None, help="End date. If not set then now is used."
    )

    args = parser.parse_args()
    # Setup logging verbosity
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, args.log_level)]

    end_date = args.enddate

    if end_date is None:
        # end_date = start_date + BATCH_TDELTA_PERIOD
        end_date = datetime.datetime.utcnow()
    else:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")

    # updateRecords()
    getPortalMetadata("urn:uuid:8cdb22c6-cb33-4553-93ca-acb6f5d53ee4")
    return


if __name__ == "__main__":
    sys.exit(main())