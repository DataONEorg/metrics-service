"""
Script to gather some system metadata properties, outputting to JSON records on disk that are
suitable for forwarding to ElasticSearch via filebeat and logstash.

Fields:

  PID: The identifier of the object for which properties are being records (typically a METADATA object)
  aggregatedBy: List of resource maps that this object appears in
  authoritativeMN: The authoritative Member Node ID
  originMN: The origin Member Node ID
  userID: List of user IDs with write access to the object
  datasetIdentifierFamily: List of identifiers that are associated with this PID

"""
import sys
import os
import argparse
import logging
import re
import json
import solrclient
import datetime
from d1_metrics_service import pid_resolution

APP_LOG = "app"
DATA_LOG = "dataset"
MAX_LOGFILE_SIZE = 1_073_741_824  # 1GB
MAX_LOG_BACKUPS = 250  # max of about 250GB of log files stored
LOGMATCH_PATTERN = '^{'

SOLR_BASE_URL = "http://localhost:8983/solr"
SOLR_CORE = "search_core"
SOLR_SELECT = "/select/"

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
      s = "%s,%03d" % (t, record.msecs)
    return s


def setupLogger(level=logging.INFO):
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    logger.setLevel(level)
    formatter = AppLogFormatter(fmt='%(asctime)s %(name)s %(levelname)s: %(message)s',
                             datefmt='%Y%m%dT%H%M%S.%f%z')
    logger = logging.getLogger(APP_LOG)
    l2 = logging.StreamHandler()
    l2.setFormatter(formatter)
    l2.setLevel(level)
    logger.addHandler(l2)


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


def getLastLinesFromFile(fname, seek_back=100000, pattern=LOGMATCH_PATTERN, lines_to_return=1):
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
    i = len(lines) - 1
    if i > lines_to_return:
        i = lines_to_return
    results = []
    while i >= 0:
        line = lines[i].decode().strip()
        if re.match(pattern, line) is not None:
            results.insert(0, line)
        i = i - 1
    return results


def parseDOI(identifier, nodeId):
    doi = None
    if identifier is None:
        return doi
    if nodeId == "urn:node:TDAR":
        #doi:10.6067:XCV8TM78S9_meta$v=1319571080230
        if identifier.startswith("doi:"):
            epos = identifier.find("_meta")
            identifier = identifier[0:epos]
            parts = identifier.split(":")
            doi = f"{parts[0]}:{parts[1]}/{parts[2]}"
    elif nodeId == "urn:node:DRYAD":
        # http://dx.doi.org/10.5061/dryad.26h4q/15?ver=2017-05-17T11:39:45.853-04:00
        epos = identifier.find("?")
        identifier = identifier[0:epos]
        doi = identifier.replace("http://dx.doi.org/","doi:")
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
        "datasetDOIFamily":[],
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
    if len(last_record_line) > 0:
        record = json.loads(last_record_line[0])
        tstamp = record["dateModified"]
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
    tstamp = datetime.datetime.strptime(tstamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    return tstamp


def getRecords(record_file_path, start_date=None, end_date=None, test_only=True):
    # modified_start = datetime.datetime.strptime("2018-11-05T18:15:00", "%Y-%m-%dT%H:%M:%S")
    # modified_end = modified_start + datetime.timedelta(minutes=15)
    L = logging.getLogger(APP_LOG)
    L.info("Start date = %s", str(start_date))
    L.info("End date = %s", str(end_date))
    recorder = getDataOutputLogger(record_file_path, log_level=logging.INFO)
    #number of entries to retrieve before resolving PIDs
    batch_size = 20

    solr_url = f"{SOLR_BASE_URL}/{SOLR_CORE}{SOLR_SELECT}"
    batch = []
    results = []
    counter = 0
    total = 0
    #getPIDList return an iterator for records of the specified period
    for entry in getPIDList(
        SOLR_BASE_URL,
        SOLR_CORE,
        SOLR_SELECT,
        modified_start=start_date,
        modified_end=end_date,
    ):
        res = getPropertiesForPID(entry, solr_url)
        results.append(res)
        batch.append(res["PID"])
        if len(batch) > batch_size:
            resolved_pids = pid_resolution.getResolvePIDs(batch, solr_url)
            for result in results:
                result["datasetIdentifierFamily"] = resolved_pids[result["PID"]]
                result["datasetDOIFamily"] = identifiersToDOIs(result["datasetIdentifierFamily"], result["originMN"])
                recorder.info(json.dumps(result))
                #print(json.dumps(result, indent=2))
            batch = []
            results = []
        counter += 1
        if counter % 100 == 0:
            L.info("Processed %d records", counter)
    # Process any remaining items
    if len(batch) > 0:
        resolved_pids = pid_resolution.getResolvePIDs(
            batch, solr_url, use_mm_params=False
        )
        for result in results:
            result["datasetIdentifierFamily"] = resolved_pids[result["PID"]]
            result["datasetDOIFamily"] = identifiersToDOIs(result["datasetIdentifierFamily"], result["originMN"])
            recorder.info(json.dumps(result))
            #print(json.dumps(result, indent=2))


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
        help="Start date. If not set then last entry is used."
    )
    parser.add_argument(
        "-E",
        "--enddate",
        default=None,
        help="End date. If not set then now is used."
    )

    args = parser.parse_args()
    # Setup logging verbosity
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(len(levels) - 1, args.log_level)]
    setupLogger(level=level)
    record_file_path = args.record_dest
    start_date = args.startdate
    end_date = args.enddate
    if start_date is None:
        start_date = findMostRecentRecord(record_file_path)
    else:
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
    if end_date is None:
        #end_date = start_date + BATCH_TDELTA_PERIOD
        end_date = datetime.datetime.utcnow()
    else:
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S")
    getRecords(record_file_path, start_date=start_date, end_date=end_date, test_only=args.test)
    return 0


if __name__ == "__main__":
    sys.exit(main())
