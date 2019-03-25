"""
Script to gather some system metadata properties, outputting to JSON records on disk that are
suitable for forwarding to ElasticSearch via filebeat and logstash.

Note: This script needs Python 3.6 or higher.

Fields:

  PID: The identifier of the object for which properties are being records (typically a METADATA object)
  aggregatedBy: List of resource maps that this object appears in
  authoritativeMN: The authoritative Member Node ID
  originMN: The origin Member Node ID
  userID: List of user IDs with write access to the object
  datasetIdentifierFamily: List of identifiers that are associated with this PID

entryType:               Constant, always = "sysmeta_ids"
PID:                     The persistent identifier

SID:                     The series ID from system metadata

DOIs:                    List of DOIs that may be the PID, and/or the SID if those
                         identifiers are DOIs. Entries in this field are processed
                         to be resolvable DOIs according to the rules applicable to
                         the Member Node.

isPublic:                indicates if the record is public, according to the solr index.

dateModified:            dateModified entry from system metadata

formatId:                The formatId from system metadata

formatType:              The formatType from system metadata

aggregatedBy:            List of ORE documents aggregating this object

authoritativeMN:         The authoritative MN as recorded in the system metadata

originMN:                The NodeId of the source Member Node

userID:                  List of users with write access

datasetIdentifierFamily: List of identifiers, as found by the algorithm
                         pid_resolution.getResolvePIDs

datasetDOIFamily:        Entries from datasetIdentifierFamily that are recognized as DOIs
                         (and processed by the DOI parser rules applicable to the Member Node)
"""
import sys
import os
import argparse
import logging
import re
import json
from . import solrclient
import datetime
import asyncio
import concurrent.futures
import requests

APP_LOG = "app"
APP_LOG_FORMATS = {
    logging.DEBUG: "%(asctime)s %(name)s.%(module)s.%(funcName)s[%(lineno)s] %(levelname)s: %(message)s",
    logging.INFO: "%(asctime)s %(name)s.%(module)s.%(funcName)s %(levelname)s: %(message)s",
    logging.WARNING: "%(asctime)s %(name)s.%(module)s.%(funcName)s %(levelname)s: %(message)s",
    logging.ERROR: "%(asctime)s %(name)s.%(module)s.%(funcName)s[%(lineno)s] %(levelname)s: %(message)s",
    logging.CRITICAL: "%(asctime)s %(name)s.%(module)s.%(funcName)s[%(lineno)s] %(levelname)s: %(message)s",
    logging.FATAL: "%(asctime)s %(name)s.%(module)s.%(funcName)s[%(lineno)s] %(levelname)s: %(message)s",
}
DATA_LOG = "dataset"
MAX_LOGFILE_SIZE = 1_073_741_824  # 1GB
MAX_LOG_BACKUPS = 250  # max of about 250GB of log files stored
LOGMATCH_PATTERN = "^{"

SOLR_BASE_URL = "http://localhost:8983/solr"
SOLR_CORE = "search_core"
SOLR_SELECT = "/select/"

CONCURRENT_REQUESTS = 20  # max number of concurrent requests to run

DEFAULT_RECORD_DEST = "dataset_info.log"

DEFAULT_FORMAT_TYPES = ["METADATA"]

SOLR_FIELDS = [
    "id",
    "isPublic",
    "seriesId",
    "dateModified",
    "formatId",
    "formatType",
    "rightsHolder",
    "authoritativeMN",
    "datasource",
    "resourceMap",
    "changePermission",
    "writePermission",
]

SOLR_SORT_SPEC = "dateModified asc"

ZERO = datetime.timedelta(0)

BATCH_TDELTA_PERIOD = datetime.timedelta(days=10)


# ==========


def quoteTerm(term):
    """
  Return a quoted, escaped Solr query term
  Args:
    term: (string) term to be escaped and quoted

  Returns: (string) quoted, escaped term
  """
    return '"' + solrclient.escapeSolrQueryTerm(term) + '"'


def _getIdsFromSolrResponse(response_text, pids=[]):
    """
  Helper to retrieve identifiers from the solr response

  Args:
    response_text: The solr response json text.
    pids: A list of identifiers to which identifiers here are added

  Returns: pids with any additional identifiers appended.
  """
    data = json.loads(response_text)
    for doc in data["response"]["docs"]:
        try:
            pid = doc["id"]
            if not pid in pids:
                pids.append(pid)
        except KeyError as e:
            pass
        try:
            for pid in doc["documents"]:
                if not pid in pids:
                    pids.append(pid)
        except KeyError as e:
            pass
        try:
            pid = doc["obsoletes"]
            if not pid in pids:
                pids.append(pid)
        except KeyError as e:
            pass
        try:
            for pid in doc["resourceMap"]:
                if not pid in pids:
                    pids.append(pid)
        except KeyError as e:
            pass
    return pids


def getResolvePIDs(PIDs, solr_url, use_mm_params=True):
    """
  Implements same functionality as metricsreader.resolvePIDs, except works asynchronously for input pids

  input: ["urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863", ]
  output: {"urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863": ["urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863",
                                                             "knb.92123.1",
                                                             "urn:uuid:d64e5f8b-c91c-487a-8ce7-0cd271194f34",
                                                             "urn:uuid:bb01b2c8-5e6c-4645-903d-39dbdd8d4d56",
                                                             "urn:uuid:d80dc5c2-bfd7-4023-87a3-9e47a2c57fbb",
                                                             "urn:uuid:9609acb1-63f2-40c6-88e3-ca9a16b06c79",
                                                             "urn:uuid:542141d3-ed5a-4d97-b759-28a17757b0b8",
                                                             "urn:uuid:22ef5022-8ade-4549-acac-c18656dd2033",
                                                             "urn:uuid:2cdf8adb-79c4-4b6c-875a-3e459c3817c7"],
          }
  Args:
    PIDs:
    solr_url:

  Returns:
  """

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
        if not "q" in paramsd:
            paramsd["q"] = "*:*"
        return session.post(url, data=paramsd)

    def _fetch(url, an_id):
        session = requests.Session()
        resMap = []
        result = []
        # always return at least this identifier
        result.append(an_id)
        params = {
            "wt": (None, "json"),
            "fl": (None, "documents,resourceMap"),
            "rows": (None, 1000),
        }
        params["fq"] = (
            None,
            "((id:" + quoteTerm(an_id) + ") OR (seriesId:" + quoteTerm(an_id) + "))",
        )
        response = _doPost(session, url, params, use_mm=use_mm_params)
        if response.status_code == requests.codes.ok:
            # continue
            logging.debug(response.text)
            resMap = _getIdsFromSolrResponse(response.text, resMap)
            more_resMap_work = True
            params["fl"] = (None, "documents,obsoletes")

            while more_resMap_work:
                current_length = len(resMap)
                query = ") OR (".join(map(quoteTerm, resMap))
                params["fq"] = (None, "id:((" + query + "))")
                response = _doPost(session, url, params, use_mm=use_mm_params)
                if response.status_code == requests.codes.ok:
                    resMap = _getIdsFromSolrResponse(response.text, resMap)
                    if len(resMap) == current_length:
                        more_resMap_work = False
                else:
                    more_resMap_work = False

            params["fl"] = (None, "id,documents,obsoletes")
            query = ") OR (".join(map(quoteTerm, resMap))
            params["fq"] = (None, "resourceMap:((" + query + "))")
            response = _doPost(session, url, params, use_mm=use_mm_params)
            if response.status_code == requests.codes.ok:
                result = _getIdsFromSolrResponse(response.text, result)

            more_work = True
            while more_work:
                current_length = len(result)
                query = ") OR (".join(map(quoteTerm, result))
                params["fq"] = (None, "id:((" + query + "))")
                response = _doPost(session, url, params, use_mm=use_mm_params)
                if response.status_code == requests.codes.ok:
                    result = _getIdsFromSolrResponse(response.text, result)
                    if len(result) == current_length:
                        more_work = False
                else:
                    more_work = False
        return result

    async def _work(pids):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=CONCURRENT_REQUESTS
        ) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            for an_id in pids:
                url = solr_url  # call here as option for RR select
                tasks.append(loop.run_in_executor(executor, _fetch, url, an_id))
            for response in await asyncio.gather(*tasks):
                results[response[0]] = response

    _L = logging.getLogger(APP_LOG)
    results = {}
    _L.debug("Enter")
    # In a multithreading environment such as under gunicorn, the new thread created by
    # gevent may not provide an event loop. Create a new one if necessary.
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError as e:
        _L.info("Creating new event loop.")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    future = asyncio.ensure_future(_work(PIDs))
    loop.run_until_complete(future)
    return results


# ==========


class UTC(datetime.tzinfo):
    """
    UTC
    """

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


class AppLogFormatter(logging.Formatter):
    converter = datetime.datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt is not None:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s.%03d" % (t, record.msecs)
        return s


def setupLogger(level=logging.INFO, name=None):
    logger = logging.getLogger(name)
    for handler in logger.handlers:
        logger.removeHandler(handler)
    log_handler = logging.StreamHandler()
    log_formatter = AppLogFormatter(
        fmt=APP_LOG_FORMATS.get(level, "%(asctime)s %(levelname)s: %(message)s"),
        datefmt="%Y%m%dT%H%M%S.%f%z",
    )
    log_handler.setFormatter(log_formatter)
    logger.addHandler(log_handler)
    logger.setLevel(level)


def getDataOutputLogger(log_file_name, log_level=logging.INFO):
    logger = logging.Logger(name=DATA_LOG)
    formatter = logging.Formatter("%(message)s")
    if log_file_name == "-":
        handler = logging.StreamHandler(sys.stderr)
    else:
        handler = logging.handlers.RotatingFileHandler(
            filename=log_file_name,
            mode="a",
            maxBytes=MAX_LOGFILE_SIZE,
            backupCount=MAX_LOG_BACKUPS,
        )
    handler.setFormatter(formatter)
    handler.setLevel(log_level)
    logger.addHandler(handler)
    return logger


def getLastLinesFromFile(
    fname, seek_back=100_000, pattern=LOGMATCH_PATTERN, lines_to_return=1
):
    """
  Returns the last lines matching pattern from the file fname

  Args:
    fname: name of file to examine
    seek_back: number of bytes to look backwards in file
    pattern: Pattern lines must match to be returned
    lines_to_return: maximum number of lines to return

  Returns:
    last n log entries that match pattern
  """
    L = logging.getLogger(APP_LOG)
    if fname == "-":
        return []
    # Does file exist?
    if not os.path.exists(fname):
        L.warning("Log file not found. Starting from zero.")
        return []
    # Do we have any interesting content in the file?
    fsize = os.stat(fname).st_size
    if fsize < 100:
        L.warning("No records in log. Starting from zero.")
        return []
    # Reduce the seek backwards if necessary
    if fsize < seek_back:
        seek_back = fsize
    # Get the last chunk of bytes from the file as individual lines
    with open(fname, "rb") as f:
        f.seek(-seek_back, os.SEEK_END)
        lines = f.readlines()
    # Find lines that match the pattern
    # i = len(lines) - 1
    # if i > lines_to_return:
    #    i = lines_to_return
    results = []
    num_lines = len(lines)
    if num_lines < lines_to_return:
        lines_to_return = num_lines
    i = 0
    while len(results) < lines_to_return:
        line = lines[num_lines - i - 1].decode().strip()
        if re.match(pattern, line) is not None:
            results.insert(0, line)
        i = i + 1
    return results


def parseDOI(identifier, nodeId):
    doi = None
    if identifier is None:
        return doi
    if nodeId == "urn:node:TDAR":
        # doi:10.6067:XCV8TM78S9_meta$v=1319571080230
        if identifier.startswith("doi:"):
            epos = identifier.find("_meta")
            identifier = identifier[0:epos]
            parts = identifier.split(":")
            doi = f"{parts[0]}:{parts[1]}/{parts[2]}"
    elif nodeId == "urn:node:DRYAD":
        # http://dx.doi.org/10.5061/dryad.26h4q/15?ver=2017-05-17T11:39:45.853-04:00
        epos = identifier.find("?")
        identifier = identifier[0:epos]
        doi = identifier.replace("http://dx.doi.org/", "doi:")
    elif nodeId == "urn:node:RW":
        # 10.24431/rw1k13
        if identifier.startswith("10.24431"):
            doi = "doi:" + identifier
    elif nodeId == "urn:node:IEDA_MGDL":
        # http://doi.org/10.1594/IEDA/312247
        doi = identifier.replace("http://doi.org/", "doi:")
    else:
        if identifier.startswith("doi:"):
            doi = identifier
    return doi


def identifiersToDOIs(identifiers, nodeId):
    result = []
    for identifier in identifiers:
        doi = parseDOI(identifier, nodeId)
        if not doi is None:
            result.append(doi)
    return result


def getEntryWritePermissions(entry):
    res = []
    try:
        res.append(entry["rightsHolder"])
    except KeyError as e:
        logging.warning(f"No rightsHolder for document {entry['id']}.")
    permissions = entry.get("changePermission", [])
    for perm in permissions:
        if perm not in res:
            res.append(perm)
    permissions = entry.get("writePermission", [])
    for perm in permissions:
        if perm not in res:
            res.append(perm)
    return res


def getPropertiesForPID(entry, solr_url):
    """
    Load system metadata for PID and return a structure that can be
    converted to JSON.

    Args:
        entry: a record from a Solr response

    Returns: dictionary
    """
    res = {
        "entryType": "sysmeta_ids",
        "PID": entry["id"],
        "SID": entry.get("seriesId", None),
        "DOIs": [],
        "isPublic": entry.get("isPublic", "false"),
        "dateModified": entry.get("dateModified", None),
        "formatId": entry.get("formatId", None),
        "formatType": entry.get("formatType", None),
        "aggregatedBy": entry.get("resourceMap", []),
        "authoritativeMN": entry.get("authoritativeMN", None),
        "originMN": entry.get("datasource", None),
        "userID": [],
        "datasetIdentifierFamily": [],
        "datasetDOIFamily": [],
    }
    res["DOIs"] = identifiersToDOIs([res["PID"], res["SID"]], res["originMN"])
    res["userID"] = getEntryWritePermissions(entry)
    # It is more efficient to gather resolved pids in batches, so is done later.
    # resolved_pids = pid_resolution.getResolvePIDs([res["PID"],], solr_url)
    # res["datasetIdentifierFamily"] = resolved_pids.get(res["PID"], res["PID"])
    return res


def getPIDList(
    solr_base,
    solr_core,
    solr_select,
    modified_start=None,
    modified_end=None,
    format_types=DEFAULT_FORMAT_TYPES,
):
    L = logging.getLogger(APP_LOG)
    solr_date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    if modified_end is None:
        modified_end = datetime.datetime.utcnow()
    if modified_start is None:
        one_day = datetime.timedelta(days=1)
        modified_start = modified_end - one_day
    formats = ""
    if len(format_types) > 0:
        formats = " OR ".join(f"formatType:{ft}" for ft in format_types)
        formats = f" AND ({formats})"
    d_start = solrclient.escapeSolrQueryTerm(modified_start.strftime(solr_date_format))
    d_end = solrclient.escapeSolrQueryTerm(modified_end.strftime(solr_date_format))
    q = "dateModified:{" + f"{d_start} TO {d_end}] {formats}"
    L.debug(q)
    return solrclient.SolrSearchResponseIterator(
        solr_base,
        solr_core,
        q,
        select=solr_select,
        fields=",".join(SOLR_FIELDS),
        sort=SOLR_SORT_SPEC,
    )


def findMostRecentRecord(record_file_path):
    """
    Find the datelastModified of the most recently retrieved record. This is used as the
    starting point for updates.

    Args:
        record_file_path: path to the file used to record the records.

    Returns: dateTime of the most recent record.
    """
    # date_from = datetime.datetime.strptime(dstring, "%Y-%m-%dT%H:%M:%S.%fZ")
    L = logging.getLogger(APP_LOG)
    tstamp = None
    last_record_line = getLastLinesFromFile(record_file_path)
    L.debug(str(last_record_line))
    if len(last_record_line) > 0:
        record = json.loads(last_record_line[0])
        tstamp = record["dateModified"]
        L.info("Retrieved record timestamp from log: %s", tstamp)
        tstamp = datetime.datetime.strptime(tstamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        return tstamp
    # Query solr for the oldest record
    params = {
        "q": "dateModified:[* TO *]",
        "fl": "dateModified",
        "sort": SOLR_SORT_SPEC,
        "start": 0,
        "rows": 1,
        "wt": "json",
        "indent": "on",
    }
    client = solrclient.SolrClient(SOLR_BASE_URL, SOLR_CORE, SOLR_SELECT)
    res = client.doGet(params)
    L.debug("Solr response = " + str(res))
    # tstamp = datetime.datetime.strptime(res)
    tstamp = res["response"]["docs"][0]["dateModified"]
    L.info("Retrieved timestamp from solr: %s", tstamp)
    tstamp = datetime.datetime.strptime(tstamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    return tstamp


def getRecords(record_file_path, start_date=None, end_date=None, test_only=True):
    # modified_start = datetime.datetime.strptime("2018-11-05T18:15:00", "%Y-%m-%dT%H:%M:%S")
    # modified_end = modified_start + datetime.timedelta(minutes=15)
    L = logging.getLogger(APP_LOG)
    L.info("Start date = %s", str(start_date))
    L.info("End date = %s", str(end_date))
    recorder = getDataOutputLogger(record_file_path, log_level=logging.INFO)
    # number of entries to retrieve before resolving PIDs
    batch_size = 20

    solr_url = f"{SOLR_BASE_URL}/{SOLR_CORE}{SOLR_SELECT}"
    batch = []
    results = []
    counter = 0
    total = 0
    # getPIDList return an iterator for records of the specified period
    iterator = getPIDList(
        SOLR_BASE_URL,
        SOLR_CORE,
        SOLR_SELECT,
        modified_start=start_date,
        modified_end=end_date,
    )
    total = iterator._num_hits
    L.info("Retrieving total of %d records", total)
    if test_only:
        L.info("Test only, exiting.")
        return counter
    for entry in iterator:
        res = getPropertiesForPID(entry, solr_url)
        results.append(res)
        batch.append(res["PID"])
        if len(batch) > batch_size:
            resolved_pids = getResolvePIDs(batch, solr_url, use_mm_params=False)
            for result in results:
                result["datasetIdentifierFamily"] = resolved_pids[result["PID"]]
                result["datasetDOIFamily"] = identifiersToDOIs(
                    result["datasetIdentifierFamily"], result["originMN"]
                )
                recorder.info(json.dumps(result))
                # print(json.dumps(result, indent=2))
            batch = []
            results = []
        counter += 1
        if counter % 100 == 0:
            L.info("Processed %d records", counter)
    # Process any remaining items
    if len(batch) > 0:
        resolved_pids = getResolvePIDs(batch, solr_url, use_mm_params=False)
        for result in results:
            result["datasetIdentifierFamily"] = resolved_pids[result["PID"]]
            result["datasetDOIFamily"] = identifiersToDOIs(
                result["datasetIdentifierFamily"], result["originMN"]
            )
            recorder.info(json.dumps(result))
            # print(json.dumps(result, indent=2))
    L.info("Done. Processed %d records.", counter)
    return counter


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
        "-f", "--record_dest", default=DEFAULT_RECORD_DEST, help="Log destination"
    )
    parser.add_argument(
        "-t",
        "--test",
        default=False,
        action="store_true",
        help="Show the starting point and number of records to retrieve but don't download.",
    )
    parser.add_argument(
        "-S",
        "--startdate",
        default=None,
        help="Start date. If not set then last entry is used.",
    )
    parser.add_argument(
        "-E", "--enddate", default=None, help="End date. If not set then now is used."
    )

    args = parser.parse_args()
    # Setup logging verbosity
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, args.log_level)]
    setupLogger(level=level, name=APP_LOG)
    record_file_path = args.record_dest
    start_date = args.startdate
    end_date = args.enddate
    if start_date is None:
        start_date = findMostRecentRecord(record_file_path)
    else:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
    if end_date is None:
        # end_date = start_date + BATCH_TDELTA_PERIOD
        end_date = datetime.datetime.utcnow()
    else:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")
    getRecords(
        record_file_path, start_date=start_date, end_date=end_date, test_only=args.test
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
