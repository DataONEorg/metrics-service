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
import itertools
import psycopg2
import requests
import asyncio
import concurrent.futures

from aiohttp import ClientSession

from collections import OrderedDict

from d1_metrics import common
from d1_metrics.solrclient import SolrClient
from d1_metrics.metricsdatabase import MetricsDatabase
from d1_metrics.metricselasticsearch import MetricsElasticSearch

from d1_metrics_service import pid_resolution

DATAONE_CN_SOLR = [
    "https://cn-secondary.dataone.org/cn/v2/query"
]

DATAONE_MN_SOLR = [
    "https://arcticdata.io/metacat/d1/mn/v2/query",
    "https://knb.ecoinformatics.org/knb/d1/mn/v2/query",
]

CONCURRENT_REQUESTS = 3  # max number of concurrent requests to run

ZERO = datetime.timedelta(0)

BATCH_TDELTA_PERIOD = datetime.timedelta(minutes=10)

DATAONE_ADMIN_SUBJECTS = ["CN=urn:node:CN,DC=dataone,DC=org","CN=urn:node:CNUNM1,DC=dataone,DC=org",
                          "CN=urn:node:CNUCSB1,DC=dataone,DC=org","CN=urn:node:CNORC1,DC=dataone,DC=org",
                          "CN=urn:node:KNB,DC=dataone,DC=org","CN=urn:node:ESA,DC=dataone,DC=org","CN=urn:node:SANPARKS,"
                          "CN=ornldaac,DC=cilogon,DC=org","CN=urn:node:LTER,DC=dataone,DC=org",
                          "CN=urn:node:CDL,DC=dataone,DC=org","CN=urn:node:PISCO,DC=dataone,DC=org",
                          "CN=urn:node:ONEShare,DC=dataone,DC=org","CN=urn:node:mnORC1,DC=dataone,DC=org",
                          "CN=urn:node:mnUNM1,DC=dataone,DC=org","CN=urn:node:mnUCSB1,DC=dataone,DC=org",
                          "CN=urn:node:TFRI,DC=dataone,DC=org","CN=urn:node:USANPN,DC=dataone,DC=org",
                          "CN=urn:node:SEAD,DC=dataone,DC=org","CN=urn:node:GOA,DC=dataone,DC=org",
                          "CN=urn:node:KUBI,DC=dataone,DC=org","CN=urn:node:LTER_EUROPE",
                          "CN=urn:node:DRYAD,DC=dataone,DC=org","CN=urn:node:CLOEBIRD,DC=dataone,DC=org",
                          "CN=urn:node:EDACGSTORE,DC=dataone,DC=org","CN=urn:node:IOE,DC=dataone,DC=org",
                          "CN=urn:node:US_MPC,DC=dataone,DC=org","CN=urn:node:EDORA,DC=dataone,DC=org",
                          "CN=urn:node:RGD,DC=dataone,DC=org","CN=urn:node:GLEON,DC=dataone,DC=org",
                          "CN=urn:node:IARC,DC=dataone,DC=org","CN=urn:node:NMEPSCOR,DC=dataone,DC=org",
                          "CN=urn:node:TERN,DC=dataone,DC=org","CN=urn:node:NKN,DC=dataone,DC=org",
                          "CN=urn:node:USGS_SDC,DC=dataone,DC=org","CN=urn:node:NRDC,DC=dataone,DC=org",
                          "CN=urn:node:NCEI,DC=dataone,DC=org","CN=urn:node:PPBIO,DC=dataone,DC=org",
                          "CN=urn:node:NEON,DC=dataone,DC=org","CN=urn:node:TDAR,DC=dataone,DC=org",
                          "CN=urn:node:ARCTIC,DC=dataone,DC=org","CN=urn:node:BCODMO,DC=dataone,DC=org",
                          "CN=urn:node:GRIIDC,DC=dataone,DC=org","CN=urn:node:R2R,DC=dataone,DC=org",
                          "CN=urn:node:EDI,DC=dataone,DC=org","CN=urn:node:UIC,DC=dataone,DC=org",
                          "CN=urn:node:RW,DC=dataone,DC=org","CN=urn:node:FEMC,DC=dataone,DC=org",
                          "CN=urn:node:OTS_NDC,DC=dataone,DC=org","CN=urn:node:PANGAEA,DC=dataone,DC=org",
                          "CN=urn:node:ESS_DIVE,DC=dataone,DC=org","CN=urn:node:CAS_CERN,DC=dataone,DC=org",
                          "CN=urn:node:FIGSHARE_CARY,DC=dataone,DC=org",
                          "CN=urn:node:mnTestIEDA_EARTHCHEM,DC=dataone,DC=org",
                          "CN=urn:node:mnTestIEDA_USAP,DC=dataone,DC=org","CN=urn:node:mnTestIEDA_MGDL,DC=dataone,DC=org",
                          "CN=urn:node:METAGRIL,DC=dataone,DC=org","CN=urn:node:ARM,DC=dataone,DC=org",
                          "CN=urn:node:CA_OPC,DC=dataone,DC=org"]

DATAONE_ADMIN_SUBJECTS_TAG = "d1_admin_subject"

DEFAULT_ELASTIC_CONFIG = {
  "host":"localhost",
  "port":9200,
  "request_timeout":30,
  }


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

    logger = logging.getLogger("es_eventlog")
    logger.setLevel(level)
    f_handler = logging.FileHandler('./es_eventlog.log')
    f_handler.setLevel(level)
    f_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_formatter)
    logger.addHandler(f_handler)
    return logger


def performRegularPortalChecks(mode="regular"):
    """
    Queries Solr to get the list of portals that were modified
    Initiates the index update procedures for modified portals
    :param: mode Indicates the mode in which the jobs will be executed
    :return: None
    """
    logger = getESSyncLogger(name="es_eventlog")
    t_start = time.time()

    logger.info("Beginning performRegularPortalChecks")

    # TODO : replace with solr to get the list of portals that needs update
    portalDict = OrderedDict()
    portalDict = performRegularPortalCollectionQueryChecks(mode=mode)

    # Test set up, not needed
    # data= testSetUP()
    # portalDict = data["portalIdentifiers"]

    logger.info("Loaded portals from Solr")

    logger.info("Beginning handle Portal Job")

    for key in portalDict:
        logger.info("Performing job for portal " + key + " with seriesId: " + portalDict[key])
        handlePortalJob(portalDict[key])
    t_delta = time.time() - t_start
    logger.info('performRegularPortalChecks:t1=%.4f', t_delta)

    return


def performRegularPortalCollectionQueryChecks(fields=None, mode=None):
    """
    Checks for newly added datasets that satisfy the collection query
    associated with this portal
    :param: fields
    :param: mode
    :return:
    """

    # Set Portal objects retieval query
    formatId = "portals"
    queryString = "label:* AND -obsoletedBy:* AND formatId: *" + formatId + "*"

    # Adding time component
    if mode is not None and mode == "new":
        queryDate = (datetime.datetime.utcnow() - BATCH_TDELTA_PERIOD)
        dateCreated = queryDate.strftime("%Y-%m-%dT%H:%M:%SZ")
        queryString += " AND dateUploaded:[%s TO NOW]" % (dateCreated)

    if fields is None:
        fields = "seriesId, label, id, collectionQuery"

    portalData = querySolr(query_string=queryString, rows="10000000", fl=fields)

    portalDict = OrderedDict()
    for portalObject in portalData["response"]["docs"]:
        portalPIDSizeQuery = portalObject["collectionQuery"]
        portalPIDSizeQueryData = querySolr(query_string=portalPIDSizeQuery, rows="0")

        if portalPIDSizeQueryData is not None and "response" in portalPIDSizeQueryData and "numFound" in portalPIDSizeQueryData["response"]:
            portalObject["PIDSize"] = portalPIDSizeQueryData["response"]["numFound"]
        else:
            portalObject["PIDSize"] = 1

    for portalObject in sorted(portalData["response"]["docs"], key=lambda k_v: k_v["PIDSize"]):
        portalDict[portalObject["label"]] = portalObject["seriesId"]

    return portalDict


def querySolr(url, query_string="*:*", wt="json", rows="1", fl=''):
    """
    Client to query solr
    :param url:
    :param query_string:
    :param wt:
    :param rows:
    :param fl:
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog")
    if url is None:
        url = DATAONE_CN_SOLR[0] + "/solr/?"

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

    logger = getESSyncLogger(name="es_eventlog")
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
    logger = getESSyncLogger(name="es_eventlog")
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
    portalIndexUpdateRequired = False

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
        updateCitationsDatabase(seriesId=seriesId, PID_List=portal_DIF)

        logger.info("Running expunge job for " + seriesId)
        updateIndex(seriesId=seriesId, PID_List=updatePortalEpungePIDs(seriesId, portal_DIF), operation="remove")

        logger.info("Running update job for " + seriesId)
        updateIndex(seriesId=seriesId, PID_List=portal_DIF, operation="add")
        storePortalHash(seriesId=seriesId,hashVal=portal_metadata["hash"], updateEntry=updateHash)

    t_delta = time.time() - t_start
    logger.info("Completed check for " + seriesId)
    logger.info('handlePortalJob:t1=%.4f', t_delta)

    return True


def updatePortalEpungePIDs(seriesId, portal_DIF):
    """
    Removes the seriesID from events for PIDs that are no longer part of the portal
    :param seriesId:
    :param portal_DIF:
    :return:
    """
    epunge_list = []
    current_portal_PIDs = []
    logger = getESSyncLogger(name="es_eventlog")
    t_start = time.time()

    logger.info("Beginning updatePortalEpungePIDs")

    metrics_elastic_search = MetricsElasticSearch()
    metrics_elastic_search.connect()

    # set up the query
    query = {
        "term": {
            "portalIdentifier.keyword": seriesId
        },
    }

    try:
        results =  metrics_elastic_search.getSearches(index="eventlog-*", q=query, fields="pid", limit=9999999)
        if results is not None:
            current_portal_PIDs = results[0]

    except Exception as e:
        logger.error("Exception occured while retrieving expunge PIDs")

    for id in current_portal_PIDs:
        if id not in portal_DIF:
            epunge_list.append(id)

    t_delta = time.time() - t_start
    logger.info("Completed check for " + seriesId)
    logger.info("Length of expunge list " + str(len(epunge_list)))
    logger.info('updatePortalEpungePIDs:t1=%.4f', t_delta)

    return epunge_list


def updateCitationsDatabase(seriesId, PID_List):
    """
    Updates the citations database table with the series identifier
    :param seriesId:
    :param PID_List:
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog")
    t_start = time.time()

    logger.info("Beginning citations database table udpates")

    # establish a connection
    metrics_database = MetricsDatabase()
    metrics_database.connect()
    csr = metrics_database.getCursor()

    # get the list of all identifiers that have citations
    sql = 'SELECT target_id FROM citations;'
    try:
        csr.execute(sql)

        if (csr.rowcount > 0):
            rows = csr.fetchall()
            citation_pid_set = set()
            for cit_tup in rows:
                citation_pid_set.add(cit_tup[0])
        logger.debug("retrieved citations successfully from the DB")

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

    # check if the identifier exist in the PID_list
    portal_citation_identifiers = set()
    for identifers in citation_pid_set:
        for portal_PID in PID_List:
            if identifers in portal_PID:
                portal_citation_identifiers.add(identifers)

    # store the seriesID for that identifier
    updateCount = 0
    for identifier in portal_citation_identifiers:
        try:

            sqlPortalId = "SELECT portal_id FROM citation_metadata WHERE target_id = '" + identifier + "';"
            csr.execute(sqlPortalId)
            rows = csr.fetchall()

            if(not rows or rows[0][0] is None):
                sql = "UPDATE citation_metadata SET portal_id = '{%s}' WHERE target_id = '%s';" % (seriesId, identifier)
            else:
                # append only if the given seriesId does not already exist in the 'portal_id' array
                sql = "UPDATE citation_metadata SET portal_id = (select array_agg(distinct e) from unnest(portal_id || '{%s}') e) where not portal_id @> '{%s}' AND target_id = '%s'; " % (seriesId, seriesId, identifier)

            csr.execute(sql)
            csrUpdateCount = csr.rowcount
            updateCount += csrUpdateCount

            if csrUpdateCount:
                logger.info("Added citation for target id - %s with series ID - %s " % (identifier, seriesId    ))
            else:
                logger.info("Target id - %s already has series ID - %s " % (identifier, seriesId))

        except psycopg2.DatabaseError as e:
            message = 'Database error! ' + str(e)
            logger.exception('Operational error!\n{0}')
            logger.exception(e)
        except psycopg2.OperationalError as e:
            logger.exception('Operational error!\n{0}')
            logger.exception(e)
        except Exception as e:
            logger.exception('Other error!\n{0}')
            logger.exception(e)
        finally:
            logger.debug("Commiting changes to DB")
            # commit
            metrics_database.conn.commit()
    logger.info("Successfully updated %s the citations pids for seriesId : %s" % (updateCount, seriesId))

    pass


def testUpdateCitationsDatabase(seriesId, PID_List):
    """
    Updates the citations database table with the series identifier
    :param seriesId:
    :param PID_List:
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog")
    t_start = time.time()

    logger.info("Beginning testing citation indexing for portal seriesId")

    # get the total count and list of all identifiers that have this seriesId

    pass


def generatePortalHash(portalDatasetIdentifierFamily=[]):
    """
    Generates hash for a given Portal from list of identifiers
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog")
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
    logger = getESSyncLogger(name="es_eventlog")
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
    Retrieves portal hash from the database
    :param seriesId:
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog")
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


def getPIDRecords(pid_sub_list, seriesId, operation):
    """
    Queries ES and retrieves records from the index for a given PID
    :return:
    """
    metrics_elastic_search = MetricsElasticSearch()
    metrics_elastic_search.connect()
    logger = getESSyncLogger(name="es_eventlog")

    # set up the query
    query = {
        "terms": {
            "pid.key": pid_sub_list
        }
    }

    must_not_query= None
    if operation == "add":
        must_not_query = {
            "terms": {
                "portalIdentifier.keyword": [
                    seriesId
                ]
            }
        }

    return metrics_elastic_search.getRawSearches(index="eventlog-*", q=query, must_not_q=must_not_query, limit=9999999)


def testSetUP():
    """
    Temporary fixture to test the functionalities
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog")
    test_fixture = {}

    # Just a random test pid from eventlog-0 index
    test_fixture["PID"] = "aekos.org.au/collection/sa.gov.au/bdbsa_veg/survey_88.20160201"
    test_fixture["portalIdentifiers"] = {}
    test_fixture["portalIdentifiers"]["icecaps"] = "urn:uuid:68ad3b4e-340e-4347-b04a-8daf65b5c65d"
    return test_fixture


async def updateRecords(seriesId="", pid_sub_list=[], session=ClientSession(), operation="add"):
    """
    Updates the record and writes it down to the ES index
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog")
    metrics_elastic_search = MetricsElasticSearch()
    metrics_elastic_search.connect()
    eventSuccessCount = 0
    eventFailCount = 0

    data = getPIDRecords(pid_sub_list, seriesId, operation)
    total_hits = 0

    index_update_seriesId = {
        "script": {
            "source": "ctx._source.portalIdentifier.add(params.tag)",
            "lang": "painless",
            "params": {
                "tag": seriesId
            }
        }
    }

    index_add_seriesId = {
        "script": "ctx._source.portalIdentifier = ['%s']" % (seriesId)
    }

    index_remove_seriesId = {
        "script": {
            "source": "if (ctx._source.portalIdentifier.contains(params.tag)) { ctx._source.portalIdentifier.remove(ctx._source.portalIdentifier.indexOf(params.tag)) }",
            "lang": "painless",
            "params": {
                "tag": seriesId
            }
        }
    }

    headers = {}
    headers["Content-Type"] = "application/x-ndjson"

    if data is not None:
        total_hits = data[1]

    if total_hits > 0:

        for data_index in range(0, len(data[0]), 500):
            try:
                bulk_update_body = ""
                for read_event_entry in itertools.islice(data[0], data_index, data_index + 500):

                    if "_id" in read_event_entry:
                        entry_id = read_event_entry["_id"]

                    if "_index" in read_event_entry:
                        read_event_entry_index = read_event_entry["_index"]
                    else:
                        logger.error("Cannot update entry: " + entry_id)
                        continue

                    update_body_syntax = {
                        "update" : {
                            "_id" : entry_id,
                            "_type" : "_doc",
                            "_index" : read_event_entry_index,
                            "retry_on_conflict" : 3
                        }
                    }
                    bulk_update_body += json.dumps(update_body_syntax) + "\n"

                    index_update_url = "http://localhost:9200/_bulk"
                    if operation == "add":
                        if "portalIdentifier" in read_event_entry["_source"]:
                            bulk_update_body += json.dumps(index_update_seriesId) + "\n"
                        else:
                            bulk_update_body += json.dumps(index_add_seriesId) + "\n"
                    elif operation == "remove":
                        bulk_update_body += json.dumps(index_remove_seriesId) + "\n"

                if ((len(seriesId) > 0) and seriesId is not None and bulk_update_body is not None):
                    bulk_update_body += "\n"
                    async with session.post(index_update_url, data=bulk_update_body,
                                            headers=headers, timeout=120) as response:
                        response_text = await response.text()
                        if response.status == 200:
                            response_data = json.loads(response_text)
                            if "items" in response_data:
                                for item in response_data["items"]:
                                    if item["update"]["status"] == 200:
                                        eventSuccessCount += 1
                                    else:
                                        logger.error(entry_id + " - " + response_text)
                        else:
                            logger.error(entry_id + " - " + response_text)

            except TimeoutError as e:
                logger.error("Timeout Error for entry_ID : " + entry_id)
            except Exception as e:
                logger.error("Exception occured for entry_ID : " + entry_id)

    return total_hits, eventSuccessCount


def updateIndex(seriesId="", PID_List=None, operation="add"):
    """
    Updates the index for the given series ID
    :param seriesId: portal Identifier to index
    :param PID_List: pids that need indexing
    :param operation: type of indexing operation
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog")
    t_start = time.time()

    totalCount = 0
    global timeoutErrorCount
    timeoutErrorCount = 0
    PID_List_iteration_count = 0
    if ((seriesId is not None) and (PID_List is not None)):
        logger.info("Updating index for " + str(seriesId))
        logger.info("Total PIDs to update:" + str(len(PID_List)))

        async def _bound_portal_updates(sem, seriesId, pid_sub_list, session, operation):
            # Getter function with semaphore.
            async with sem:
                total_hits, eventSuccessCount = await updateRecords(seriesId, pid_sub_list, session, operation)

            return total_hits, eventSuccessCount

        # Async PID updates
        async def _work(seriesId, PID_List, operation, loop):
            logger.info("Beginning async work")

            # create instance of Semaphore
            sem = asyncio.Semaphore(5)

            # Create client session that will ensure we dont open new connection
            # per each request.
            async with ClientSession(loop=loop) as session:
                PID_List_iteration_count = 0
                totalWorkCount = 0
                tasks = []

                for pid_index in range(0,len(PID_List), 1000):
                    pid_sub_list = PID_List[pid_index:pid_index+1000]
                    task = asyncio.ensure_future(_bound_portal_updates(sem, seriesId, pid_sub_list, session, operation))
                    tasks.append(task)

                logger.info("Entire PID_List tasks initialized")

                responses = asyncio.gather(*tasks)
                for resp in await responses:
                    # Logging and status tracking
                    PID_List_iteration_count += 1000
                    PID_List_iteration_count = min(PID_List_iteration_count, len(PID_List))
                    if ((PID_List_iteration_count % 100 == 0) or  (PID_List_iteration_count == len(PID_List))):
                        logger.info(str(PID_List_iteration_count) + " of " + str(len(PID_List)) + " updated; impacting "
                                    + str(totalWorkCount) + " events in the eventlog-* index")

                    totalCountList.append(resp)
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

        future = asyncio.ensure_future(_work(seriesId, PID_List, operation, loop))
        loop.run_until_complete(future)
        logger.debug("elapsed:%fsec", time.time() - t_start)

    logger.info("Updated total of " + str(sum(i[0] for i in totalCountList)) + " events for seriesId: " + seriesId)
    logger.info("Successful event writes " + str(sum(i[1] for i in totalCountList)) + " for seriesId: " + seriesId)
    logger.info("Failed event writes " + str(sum(i[0] for i in totalCountList) - sum(i[1] for i in totalCountList)) + " for seriesId: " + seriesId)
    # logger.info("Updated total of " + str(sum(totalCountList)) + " events for seriesId: " + seriesId)

    t_delta = time.time() - t_start
    logger.info("Updated index for " + seriesId)
    logger.info('updateIndex:t1=%.4f', t_delta)

    return


def subjectFiltering():
    """
    Filters the results based on the predefined admin subjects
    :return:
    """
    logger = getESSyncLogger(name="es_eventlog")
    data, total_hits = getAdminSubjects()
    updateTagsForAdminSubject(data, total_hits)


def getAdminSubjects():
    """
    Returns admin tag from the ES index
    :return:
    """
    metrics_elastic_search = MetricsElasticSearch()
    metrics_elastic_search.connect()
    logger = getESSyncLogger(name="es_eventlog")
    logger.info("Getting the admin subjects")

    query = [
        {
            "exists": {
                "field": "sessionId"
            }
        },
        {
            "terms": {
                "subject.key": DATAONE_ADMIN_SUBJECTS
            }
        }
    ]

    return metrics_elastic_search.getRawSearches(index="eventlog-*", q=query, limit=1000000)


def updateTagsForAdminSubject(data, total_hits):
    logger = getESSyncLogger(name="es_eventlog")
    t_start = time.time()

    async def _update_tags(tag_label, read_event_entry, session):
        logger = getESSyncLogger(name="es_eventlog")
        metrics_elastic_search = MetricsElasticSearch()
        metrics_elastic_search.connect()
        eventSuccessCount = 0
        writeNotNeeded = 0
        entry_id = None

        if "_id" in read_event_entry:
            entry_id = read_event_entry["_id"]

        try:
            tags_array = []

            if entry_id is None:
                logger.error("Cannot update entry without ID")

            if "_index" in read_event_entry:
                read_event_entry_index = read_event_entry["_index"]
            else:
                logger.error("Cannot update entry: " + entry_id)

            # set up the URL
            index_update_url = "http://localhost:9200/%s/_doc/%s/_update" % (read_event_entry_index, entry_id)

            headers = {}
            headers["Content-Type"] = "application/json"

            if (tag_label is not None):

                index_update_body = {
                    "script": {
                        "source": "ctx._source.tags.add(params.tag)",
                        "lang": "painless",
                        "params": {
                            "tag": tag_label
                        }
                    }
                }

                async with session.post(index_update_url, data=json.dumps(index_update_body), headers=headers) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        eventSuccessCount += 1

        except Exception as e:
            eventFailCount += 1
            logger.error(e)
            logger.info("Error occured for entry id: " + str(entry_id))
            logger.info(eventSuccessCount + " " + eventFailCount + " " + writeNotNeeded)
        return eventSuccessCount, eventFailCount, writeNotNeeded

    async def _bound_update_tags(sem, tag_label, event, session):
        # Getter function with semaphore.
        async with sem:
            await _update_tags(tag_label, event, session)

    # Async event updates
    async def _work(data):
        logger.info("Beginning async work")

        tasks = []
        tag_label = DATAONE_ADMIN_SUBJECTS_TAG

        # create instance of Semaphore
        sem = asyncio.Semaphore(10)

        # Create client session that will ensure we dont open new connection
        # per each request.
        async with ClientSession() as session:
            for event in data:
                task = asyncio.ensure_future(_bound_update_tags(sem, tag_label, event, session))
                tasks.append(task)

            responses = asyncio.gather(*tasks)
            await responses


    totalCountList = []
    # In a multithreading environment such as under gunicorn, the new thread created by
    # gevent may not provide an event loop. Create a new one if necessary.
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError as e:
        logger.info("Creating new event loop.")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    future = asyncio.ensure_future(_work(data))
    loop.run_until_complete(future)

    logger.debug("elapsed:%fsec", time.time() - t_start)
    t_delta = time.time() - t_start

    logger.info("Updated index for DATAONE" )
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

    parser.add_argument(
        "-m", "--mode", default="regular", help="Arg to indicate the type of run. eg. nightly job"
    )

    args = parser.parse_args()

    # Setup logging verbosity
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, args.log_level)]

    end_date = args.enddate
    mode = args.mode

    if end_date is None:
        # end_date = start_date + BATCH_TDELTA_PERIOD
        end_date = datetime.datetime.utcnow()
    else:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")

    if mode is not None and (mode == "regular" or mode == "new"):
        if mode == "regular":
            print("Running regular job")
        if mode == "new":
            print("Running new job")
        performRegularPortalChecks(mode=mode)

    return


if __name__ == "__main__":
    sys.exit(main())
