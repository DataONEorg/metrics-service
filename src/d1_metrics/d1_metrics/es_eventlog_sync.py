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
from d1_metrics.metricsdatabase import MetricsDatabase
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


def getESSyncLogger(level=logging.DEBUG, name=None):
    """
    Returns the logging object
    :param level:
    :param name:
    :return:
    """
    logger = logging.getLogger(name)
    for handler in logger.handlers:
        logger.removeHandler(handler)

    logger = logging.getLogger("es_eventlog_sync")
    logger.setLevel(level)
    f_handler = logging.FileHandler('./es_eventlog.log')
    f_handler.setLevel(level)
    f_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_formatter)
    logger.addHandler(f_handler)
    return logger


def getModifiedPortals():
    """
    Returns list of portals that were modified since last check
    :return:
    """
    pass


def performRegularPortalChecks():
    """
    Queries Solr to get the list of portals that were modified
    Initiates the index update procedures for modified portals
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    # TODO : replace with solr to get the list of portals that needs update
    test_fixture = testSetUP()
    logger.info("Fixture set up complete")

    logger.info("Beginning handle Portal Job")
    for key in test_fixture["portalIdentifiers"]:
        logger.info("Performing job for seriesId: " + test_fixture["portalIdentifiers"][key])
        handlePortalJob(test_fixture["portalIdentifiers"][key])
    return


def performRegularPortalCollectionQueryChecks():
    """
    Checks for newly added datasets that satisfy the collection query
    associated with this portal
    :return:
    """
    pass


def querySolr(url="https://cn.dataone.org/cn/v2/query/solr/?", query_string="*:*", wt="json", rows="1", fl=''):
    """

    :param url:
    :param wt:
    :param rows:
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    if url is None:
        url = "https://cn.dataone.org/cn/v2/query/solr/?"

    query = "q=" + query_string + "&fl=" + fl + "&wt=" + wt + "&rows=" + rows

    try:
        response = requests.get(url=url + query)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception("Error while wuerying solr. Service received Non OK status code")
    except Exception as e:
        logger.error(msg=e)
        return None


def getPortalMetadata(seriesId="", fl="seriesId,collectionQuery"):
    """
    Retrieves portal attributes from solr
    :return:
    """

    logger = getESSyncLogger(name="es_eventlog_sync")
    query_string = '(seriesId:"' + seriesId + '" AND -obsoletedBy:* AND formatType:METADATA)'
    portal_metadata = {}

    try:
        solr_response = querySolr(query_string=query_string, fl=fl)

        if solr_response["response"]["numFound"] > 0:
            portal_metadata["seriesId"] = solr_response["response"]["docs"][0]["seriesId"]
            portal_metadata["collectionQuery"] = solr_response["response"]["docs"][0]["collectionQuery"]

            # Getting all the pids related to the portal
            portal_metadata["collectionQuery"] = portal_metadata["collectionQuery"].replace('-obsoletedBy:* AND ', '')
            return portal_metadata

    except Exception as e:
        logger.error(msg=e)
        return {}



def handlePortalJob(seriesId=""):
    """
    High level function that handles the entire process of keeping ES index upto date for a given portal
    :param seriesId:
    :return:
    """

    logger = getESSyncLogger(name="es_eventlog_sync")
    # Initializing the lists
    portal_collection_PIDs = []
    portal_DIF = []
    total_portal_DIF_count = 0

    logger.info(seriesId)

    portal_metadata = getPortalMetadata(seriesId=seriesId)

    logger.info(portal_metadata)

    # resolving collection query and getting the PIDs from identifiers index
    portal_collection_PIDs = pid_resolution.resolveCollectionQueryFromSolr(collectionQuery=portal_metadata["collectionQuery"])
    logger.info(len(portal_collection_PIDs))
    portal_DIF, total_portal_DIF_count = pid_resolution.getAsyncPortalDatasetIdentifierFamilyByBatches(portal_collection_PIDs)

    logger.info("count = " + str(total_portal_DIF_count))

    # generate hash
    portal_metadata["hash"] = generatePortalHash(portal_DIF)
    logger.info("hash = " + str(portal_metadata["hash"]))

    # check in the DB

    # update if required; else continue


    return


def generatePortalHash(portalDatasetIdentifierFamily=[]):
    """
    Generates hash for a given Portal from list of identifiers
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    logger.info("Generating portal collection hash")
    portalDatasetIdentifierFamily.sort(key = str)
    return hash(tuple(portalDatasetIdentifierFamily))


def storePortalHash():
    """
    Stores portal hash in the database
    :return:
    """
    pass


def retrievePortalHash(seriesId="", metrics_database=None):
    """

    :param seriesId:
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    logger.info("Beginning hash retrieval job")
    if metrics_database is None:
        metrics_database = MetricsDatabase()
        metrics_database.connect()
    csr = metrics_database.getCursor()
    sql = 'SELECT seriesId, hash FROM portal_metadata_test;'

    portal_hash = {}
    try:
        csr.execute(sql)
        rows = csr.fetchall()
    except Exception as e:
        logger.error("Exception occured while performing DB operation : " + e)
    return portal_hash

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
    logger = getESSyncLogger(name="es_eventlog_sync")
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
    logger = getESSyncLogger(name="es_eventlog_sync")
    metrics_elastic_search = MetricsElasticSearch()
    metrics_elastic_search.connect()
    results_tuple = getPIDRecords()
    test_fixtures = testSetUP()
    total_count = len(results_tuple["hits"]["hits"])

    for read_event_entry in results_tuple["hits"]["hits"]:

        portalIdentifierArray = []
        if "portalIdentifier" in read_event_entry["_source"]:
            portalIdentifierArray = read_event_entry["_source"]["portalIdentifier"]

        portalIdentifierArray.append(test_fixtures["portalIdentifiers"]["DBO"])
        read_event_entry["_source"]["portalIdentifier"] = portalIdentifierArray

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
    # getPortalMetadata(seriesId="urn:uuid:8cdb22c6-cb33-4553-93ca-acb6f5d53ee4")
    performRegularPortalChecks()
    return


if __name__ == "__main__":
    sys.exit(main())