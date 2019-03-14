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
import logging
import json
import solrclient
import datetime
from d1_metrics_service import pid_resolution

DEFAULT_FORMAT_TYPES = [
    "METADATA",
]

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
    res = {"PID":entry["id"],
           "SID": entry.get("seriesId", None),
           "isPublic": entry.get("isPublic", "false"),
           "aggregatedBy": entry.get("resourceMap", []),
           "authoritativeMN": entry.get("authoritativeMN", None),
           "originMN": entry.get("datasource", None),
           "userID": [],
           "datasetIdentifierFamily":[],
           }
    res["userID"] = getEntryWritePermissions(entry)
    #resolved_pids = pid_resolution.getResolvePIDs([res["PID"],], solr_url)
    #res["datasetIdentifierFamily"] = resolved_pids.get(res["PID"], res["PID"])
    return res


def getPIDList(solr_base, solr_core, solr_select, modified_start=None, modified_end=None, format_types=DEFAULT_FORMAT_TYPES):
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
    q = f"dateModified:[{d_start} TO {d_end}]{formats}"
    logging.debug(q)
    return solrclient.SolrSearchResponseIterator(solr_base, solr_core, q, select=solr_select, fields=",".join(SOLR_FIELDS))


def main(solr_base, solr_core, solr_select):
    modified_start = None
    modified_end = None
    #modified_start = datetime.datetime.strptime("2018-11-05T18:15:00", "%Y-%m-%dT%H:%M:%S")
    #modified_end = modified_start + datetime.timedelta(minutes=5)
    batch_size = 10
    solr_url = f"{solr_base}/{solr_core}{solr_select}"
    batch = []
    results = []
    for entry in getPIDList(solr_base, solr_core, solr_select, modified_start=modified_start, modified_end=modified_end):
        res = getPropertiesForPID(entry, solr_url)
        results.append(res)
        batch.append(res["PID"])
        if len(batch) > batch_size:
            resolved_pids = pid_resolution.getResolvePIDs(batch, solr_url)
            for result in results:
                result["datasetIdentifierFamily"] = resolved_pids[result["PID"]]
                print(json.dumps(result, indent=2))
            batch = []
            results = []
    if len(batch) > 0:
        resolved_pids = pid_resolution.getResolvePIDs(batch, solr_url, use_mm_params=False)
        for result in results:
            result["datasetIdentifierFamily"] = resolved_pids[result["PID"]]
            print(json.dumps(result, indent=2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    solr_base = "http://localhost:8983/solr"
    solr_core = "search_core"
    solr_select = "/select/"
    sys.exit(main(solr_base, solr_core, solr_select))

