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
import asyncio
import concurrent.futures

from aiohttp import ClientSession

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

CONCURRENT_REQUESTS = 10  # max number of concurrent requests to run

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

    logger.info("Beginning performRegularPortalChecks")

    # TODO : replace with solr to get the list of portals that needs update
    portalDict = performRegularPortalCollectionQueryChecks()
    logger.info("Loaded portals from Solr")

    logger.info("Beginning handle Portal Job")

    for key in portalDict:
        logger.info("Performing job for portal " + key + " with seriesId: " + portalDict[key])
        handlePortalJob(portalDict[key])
    t_delta = time.time() - t_start
    logger.info('performRegularPortalChecks:t1=%.4f', t_delta)

    return


def performRegularPortalCollectionQueryChecks(fields=None):
    """
    Checks for newly added datasets that satisfy the collection query
    associated with this portal
    :return:
    """

    # TODO Add time component

    # Set Portal objects retieval query
    formatId = "https://purl.dataone.org/portals-1.0.0"
    queryString = "label:* AND -obsoletedBy:* AND formatId:\"" + formatId + "\""

    if fields is None:
        fields = "seriesId, label, id"

    portalData = querySolr(query_string=queryString, rows="10000000", fl=fields)

    portalDict = {}
    for portalObject in portalData["response"]["docs"]:
        portalDict[portalObject["label"]] = portalObject["seriesId"]

    return portalDict


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
            logger.error(response.text)
            raise Exception("Error while querying solr. Service received Non OK status code")
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

    logger.info("Beginning performRegularPortalChecks")

    # Initializing the lists
    portal_collection_PIDs = []
    portal_DIF = []
    PDIF = set()
    total_portal_DIF_count = 0
    updateHash = False

    logger.info(seriesId)

    portal_metadata = getPortalMetadata(seriesId=seriesId)
    portalIndexUpdateRequired = True

    logger.info(portal_metadata)

    # resolving collection query and getting the PIDs from identifiers index
    portal_collection_PIDs = pid_resolution.resolveCollectionQueryFromSolr(collectionQuery=portal_metadata["collectionQuery"])
    logger.debug("total PIDs after resolving collection query: " + str(len(portal_collection_PIDs)))

    PDIF, total_portal_DIF_count = pid_resolution.getAsyncPortalDatasetIdentifierFamilyByBatches(portal_collection_PIDs)

    portal_DIF = list(PDIF)
    logger.debug("total matches found in indentifiers-* index: " + str(total_portal_DIF_count))
    logger.debug("length of list DIF: " + str(len(portal_DIF)))

    # generate hash
    portal_metadata["hash"] = generatePortalHash(portalDatasetIdentifierFamily=portal_DIF)
    logger.info("hash = " + str(portal_metadata["hash"]))

    # check in the DB
    logger.info("Evaluating previous stored hash.")
    previousPortalCheckHash = retrievePortalHash(seriesId=seriesId)
    logger.info("prev hash : " + previousPortalCheckHash)

    if ((previousPortalCheckHash is None) or (previousPortalCheckHash != portal_metadata["hash"])):
        updateHash = True
        portalIndexUpdateRequired = True

        if (previousPortalCheckHash is None):
            logger.info("Hash not found for : " + seriesId)
            logger.info("Hash check indicates the index is out of date for seriesId : " + seriesId)
            updateHash = False

    # update if required; else continue
    if(portalIndexUpdateRequired):
        updateIndex(seriesId=seriesId, PID_List=portal_DIF)
        # storePortalHash(seriesId=seriesId,hashVal=portal_metadata["hash"], updateEntry=updateHash)

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
    portalDatasetIdentifierFamily.sort()
    tuple_portalDatasetIdentifierFamily = tuple(portalDatasetIdentifierFamily)

    m = hashlib.md5()
    for identifier in tuple_portalDatasetIdentifierFamily:
        m.update(identifier.encode("utf-8"))
    return m.hexdigest()


def storePortalHash(seriesId="", hashVal=None, metrics_database=None, updateEntry=False):
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
    if not updateEntry:
        sql = "INSERT INTO portal_metadata_test (id, series_id, hash) VALUES (DEFAULT , '" + seriesId + "','" + hashVal + "');"
    else:
        sql = "UPDATE portal_metadata_test SET hash = '" + hashVal + "' WHERE series_id = '" + seriesId + "';"

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
    logger = getESSyncLogger(name="es_eventlog_sync")

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
    # test_fixture["portalIdentifiers"]["CALM"] = "urn:uuid:77fc14cc-b69d-4e21-8c5a-1e9cdc79ad4b"
    return test_fixture


def updateRecords(seriesId="", PID=""):
    """
    Updates the record and writes it down to the ES index
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    metrics_elastic_search = MetricsElasticSearch()
    metrics_elastic_search.connect()
    eventSuccessCount = 0
    eventFailCount = 0
    writeNotNeeded = 0

    data = getPIDRecords(PID)
    total_hits = 0

    if data is not None:
        total_hits = data[1]

    if total_hits > 0:

        for read_event_entry in data[0]:
            try:
                portalIdentifierArray = []

                if "_index" in read_event_entry:
                    read_event_entry_index = read_event_entry["_index"]
                else:
                    logger.error("Cannot update entry: " + read_event_entry["_id"])
                    eventFailCount += 1
                    continue

                if ((len(seriesId) > 0) and seriesId is not None ):
                    if "portalIdentifier" in read_event_entry["_source"]:
                        portalIdentifierArray = read_event_entry["_source"]["portalIdentifier"]
                    else:
                        read_event_entry["_source"]["portalIdentifier"] = []

                    if seriesId not in portalIdentifierArray:
                        portalIdentifierArray.append(seriesId)
                        read_event_entry["_source"]["portalIdentifier"].extend(portalIdentifierArray)

                        updateStat = metrics_elastic_search.updateEvents(read_event_entry_index, read_event_entry)
                        if updateStat:
                            eventSuccessCount += 1
                        else:
                            eventFailCount += 1
                    else:
                        writeNotNeeded += 1
            except Exception as e:
                logger.error(e)
                logger.info("Error occured here: " + json.dumps(read_event_entry, indent=2))

    return total_hits, eventSuccessCount, eventFailCount, writeNotNeeded


def updateIndex(seriesId="", PID_List=None):
    """
    Updates the index for the given series ID
    :param seriesId:
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog_sync")
    t_start = time.time()

    # TODO include add or remove operation to make appropriate changes for that PID

    totalCount = 0
    PID_List_iteration_count = 0
    if( (seriesId is not None) and (PID_List is not None)):
        logger.info("Updating index for " + str(seriesId))
        logger.info("Total PIDs to update:" + str(len(PID_List)))

        # TODO get the existing list

        # TODO check for PIDs that need updating

        # Async PID updates
        async def _work(seriesId, PID_List):
            logger.info("Beginning async work")
            with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
                PID_List_iteration_count = 0
                totalWorkCount = 0
                loop = asyncio.get_event_loop()
                tasks = []

                for PID in PID_List:
                    tasks.append(loop.run_in_executor(executor, updateRecords, seriesId, PID))

                logger.info("Entire PID_List tasks initialized")
                for resp in await asyncio.gather(*tasks):
                    # Logging and status tracking
                    PID_List_iteration_count += 1
                    if (PID_List_iteration_count % 100 == 0):
                        logger.info(str(PID_List_iteration_count) + " of " + str(len(PID_List)) + " updated; impacting "
                                    + str(totalWorkCount) + " events in the eventlog-* index")

                    totalCountList.append(resp)
                    # totalWorkCount += resp[0]
                    totalWorkCount += resp[0]

        totalCountList = []
        # In a multithreading environment such as under gunicorn, the new thread created by
        # gevent may not provide an event loop. Create a new one if necessary.
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError as e:
            logger.info("Creating new event loop.")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        future = asyncio.ensure_future(_work(seriesId, PID_List))
        loop.run_until_complete(future)
        logger.debug("elapsed:%fsec", time.time() - t_start)

    logger.info("Updated total of " + str(sum(i[0] for i in totalCountList)) + " events for seriesId: " + seriesId)
    logger.info("Successful event writes " + str(sum(i[1] for i in totalCountList)) + " for seriesId: " + seriesId)
    logger.info("Failed event writes " + str(sum(i[2] for i in totalCountList)) + " for seriesId: " + seriesId)
    logger.info("Event already updated " + str(sum(i[3] for i in totalCountList)) + " for seriesId: " + seriesId)
    # logger.info("Updated total of " + str(sum(totalCountList)) + " events for seriesId: " + seriesId)

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
    # performRegularPortalCollectionQueryChecks()

    # print(json.dumps(getPIDRecords(PID="doi: 10.18739/A2F320"), indent=2))

    # storePortalHash(seriesId="urn:uuid:8cdb22c6-cb33-4553-93ca-acb6f5d53ee4", hashVal="8914987b3afe11cad14010e417e37111")
    # print(retrievePortalHash(seriesId="urn:uuid:8cdb22c6-cb33-4553-93ca-acb6f5d53ee4"))
    return


if __name__ == "__main__":
    sys.exit(main())
