"""

Script to keep the ES eventlog index upto date with the aggregated objects like -
DataONE portal objects, DataONE User profiles, etc...

Note: This script needs Python 3.6 or higher.

"""

import os
import re

import sys
import json
import time

import asyncio
import hashlib
import logging

import argparse
import datetime
import psycopg2
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
    t_start = time.time()
    t_delta = time.time() - t_start
    logger.info("Beginning performRegularPortalChecks")

    # TODO : replace with solr to get the list of portals that needs update
    test_fixture = testSetUP()
    logger.info("Fixture set up complete")

    logger.info("Beginning handle Portal Job")
    for key in test_fixture["portalIdentifiers"]:
        logger.info("Performing job for seriesId: " + test_fixture["portalIdentifiers"][key])
        handlePortalJob(test_fixture["portalIdentifiers"][key])
    t_delta = time.time() - t_start
    logger.info('performRegularPortalChecks:t1=%.4f', t_delta)

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
    t_start = time.time()
    t_delta = time.time() - t_start
    logger.info("Beginning performRegularPortalChecks")

    # Initializing the lists
    portal_collection_PIDs = []
    portal_DIF = []
    PDIF = set()
    total_portal_DIF_count = 0

    logger.info(seriesId)

    portal_metadata = getPortalMetadata(seriesId=seriesId)
    portalIndexUpdateRequired = True

    logger.info(portal_metadata)

    # resolving collection query and getting the PIDs from identifiers index
    portal_collection_PIDs = pid_resolution.resolveCollectionQueryFromSolr(collectionQuery=portal_metadata["collectionQuery"])
    logger.info(len(portal_collection_PIDs))
    PDIF, total_portal_DIF_count = pid_resolution.getAsyncPortalDatasetIdentifierFamilyByBatches(portal_collection_PIDs)

    portal_DIF = list(PDIF)
    logger.info("count = " + str(total_portal_DIF_count))

    # generate hash
    portal_metadata["hash"] = generatePortalHash(portalDatasetIdentifierFamily=portal_DIF)
    logger.info("hash = " + str(portal_metadata["hash"]))

    # check in the DB
    logger.info("Evaluating previous stored hash.")
    previousPortalCheckHash = retrievePortalHash(seriesId=seriesId)
    if ((previousPortalCheckHash is None) or (previousPortalCheckHash != portal_metadata["hash"])):
        logger.info("Hash check indicates the index is out of date for seriesId : " + seriesId)
        portalIndexUpdateRequired = True

    # update if required; else continue
    if(portalIndexUpdateRequired):
        updateIndex(seriesId=seriesId, PID_List=portal_DIF)

    t_delta = time.time() - t_start
    logger.info("Completed check for " + seriesId)
    logger.info('handlePortalJob:t1=%.4f', t_delta)

    return True


def generatePortalHash(portalDatasetIdentifierFamily=[]):
    """
    Generates hash for a given Portal from list of identifiers
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    logger.info("Generating portal collection hash")
    logger.info(str(len(portalDatasetIdentifierFamily)))
    portalDatasetIdentifierFamily.sort()
    tuple_portalDatasetIdentifierFamily = tuple(portalDatasetIdentifierFamily)

    m = hashlib.md5()
    for identifier in tuple_portalDatasetIdentifierFamily:
        m.update(identifier.encode("utf-8"))
    return m.hexdigest()


def storePortalHash(seriesId="", hashVal=None, metrics_database=None):
    """
    Stores portal hash in the database
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    logger.info("Trying to store portal hash")
    if metrics_database is None:
        metrics_database = MetricsDatabase()
        metrics_database.connect()
    csr = metrics_database.getCursor()
    sql = "INSERT INTO portal_metadata_test (id, series_id, hash) VALUES (DEFAULT , '" + seriesId + "','" + hashVal + "');"

    try:
        csr.execute(sql)
    except psycopg2.DatabaseError as e:
        message = 'Database error! ' + str(e)
        logger.exception('Operational error!\n{0}')
        logger.exception(e)
    except psycopg2.OperationalError as e:
        logger.exception('Operational error!\n{0}')
        logger.exception(e)
    finally:
        logger.info("Commiting changes to DB")
        metrics_database.conn.commit()
    return None


def retrievePortalHash(seriesId="", metrics_database=None):
    """
    Retrieves portal hash from the
    :param seriesId:
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    logger.info("Beginning hash retrieval job")
    if metrics_database is None:
        metrics_database = MetricsDatabase()
        metrics_database.connect()
    csr = metrics_database.getCursor()
    sql = 'SELECT series_id, hash FROM portal_metadata_test;'

    portal_hash = None
    try:
        csr.execute(sql)

        if (csr.rowcount > 0):
            rows  = csr.fetchall()
            portal_hash = rows[0][1]
    except psycopg2.DatabaseError as e:
        message = 'Database error! ' + str(e)
        logger.exception('Operational error!\n{0}')
        logger.exception(e)
    except psycopg2.OperationalError as e:
        logger.exception('Operational error!\n{0}')
        logger.exception(e)
    finally:
        logger.info("Commiting changes to DB")
        metrics_database.conn.commit()
    return portal_hash


def getPIDRecords(PID):
    """
    Queries ES and retrieves records from the index for a given PID
    :return:
    """
    metrics_elastic_search = MetricsElasticSearch()
    metrics_elastic_search.connect()

    # set up the query
    query = {
        "term": {
            "pid.key": PID
        }
    }

    return metrics_elastic_search.getRawSearches(index="eventlog-*", q=query, limit=9999999)


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


def updateRecords(seriesId="", PID=""):
    """
    Updates the record and writes it down to the ES index
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    metrics_elastic_search = MetricsElasticSearch()
    metrics_elastic_search.connect()
    data = getPIDRecords(PID)
    total_hits = 0
    if data is not None:
        total_hits = data["hits"]["total"]

    if total_hits > 0:
        for read_event_entry in data["hits"]["hits"]:
            portalIdentifierArray = []

            if "_index" in read_event_entry:
                read_event_entry_index = read_event_entry["_index"]
            else:
                logger.error("Cannot update entry: " + read_event_entry["_id"])
                continue

            if ((len(seriesId) > 0) and seriesId is not None ):
                if "portalIdentifier" in read_event_entry["_source"]:
                    portalIdentifierArray = read_event_entry["_source"]["portalIdentifier"]

                if seriesId not in portalIdentifierArray:
                    portalIdentifierArray.append(seriesId)
                    read_event_entry["_source"]["portalIdentifier"] = portalIdentifierArray

                    metrics_elastic_search.updateRecord(read_event_entry_index, read_event_entry)

    return total_hits


def updateIndex(seriesId="", PID_List=None):
    """
    Updates the index for the given series ID
    :param seriesId:
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    t_start = time.time()
    t_delta = time.time() - t_start


    # TODO include add or remove operation to make appropriate changes for that PID

    totalCount = 0
    PID_List_iteration_count = 0
    if( (seriesId is not None) and (PID_List is not None)):
        logger.info("Updating idnex for " + str(seriesId))
        logger.info("Total PIDs to update:" + str(len(PID_List)))

        # TODO get the existing list

        # TODO check for PIDs that need updating


        for identifier in PID_List:
            PID_List_iteration_count+=1
            if(PID_List_iteration_count %100 == 0):
                logger.info(str(PID_List_iteration_count) + " of " + str(len(PID_List)) + " updated; impacting "
                            + str(totalCount) + " events in the eventlog-* index")
            totalCount += updateRecords(seriesId=seriesId, PID=identifier)


    logger.info("Updated total of " + str(totalCount) + " records for seriesId: " + seriesId)

    t_delta = time.time() - t_start
    logger.info("Updated index for " + seriesId)
    logger.info('updateIndex:t1=%.4f', t_delta)

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

    # storePortalHash(seriesId="urn:uuid:8cdb22c6-cb33-4553-93ca-acb6f5d53ee4", hashVal="8914987b3afe11cad14010e417e37111")
    # print(retrievePortalHash(seriesId="urn:uuid:8cdb22c6-cb33-4553-93ca-acb6f5d53ee4"))
    return


if __name__ == "__main__":
    sys.exit(main())
