"""

"""


from datetime import datetime

# Python-dateutil 2.X required for Python 3.X
# Python-dateutil 1.X required for Python 2.X
from dateutil import parser as dateparser
from dateutil.tz import tzutc
from elasticsearch5 import Elasticsearch
# from luqum.parser import parser as solrparser
# from luqum.parser import ParseError

import argparse
import json
import re
import sys


# Globals
ES = Elasticsearch()
SESSION_ID = None
HOURLY_SESSION_ID = None
TTL_MINUTES = 15
TTL_MINUTES60 = 60


def main(args):
    args = parse_args(args)
    SESSION_ID = generate_next_sessionid(args.indexname)
    exitcode = 1

    if args.delete:
        exitcode = delete_index(args.indexname)

    elif args.init:
        exitcode = init_index(args.indexname)

    elif args.process:
        exitcode = processlog(args.indexname)


    else:
        print(args)
        exitcode = 0

    sys.exit(exitcode)


def processlog(indexname):
    batchsize = 500
    counter = 0
    sessionType = "hourlySessionId"

    ES.indices.refresh(indexname)
    count = get_count_unprocessed_event(indexname, sessionType)
    # print 'unprocessed count', count
    total_batches = count / batchsize + bool(count % batchsize)
    # print 'total_batches', total_batches

    while True:
        ES.indices.refresh(indexname)
        mark = get_first_unprocessed_event_datetime(indexname, sessionType)
        if mark is None:
            return 0
        print('mark', mark)

        live_sessions = get_live_sessions_before_mark(indexname, mark, sessionType)
        # print 'live_sessions', live_sessions

        new_events = get_new_events(indexname, sessionType, batchsize)
        # print 'new_events', new_events

        # process_new_events(indexname, new_events, live_sessions)
        process_new_events(indexname, new_events, live_sessions, sessionType)

        print('processed batch', counter, 'of', total_batches)
        counter = counter + 1
        # return 1

    return 1


def remove_stale_sessionids(indexname, ip, timestamp, sessionType):
    searchbody = {
        "script": {
            "inline": "ctx._source.remove(" + sessionType + ")",
            "lang": "painless"
        },
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"_type": "logevent"}
                    },
                    {
                        "exists": {
                            "field": sessionType
                        }
                    },
                    {
                        "range": {
                            sessionType: {"gt": 0}
                        }
                    },
                    {
                        "term": {"geoip.ip": ip}
                    },
                    {
                        "range": {
                            "@timestamp": {"gt": timestamp.isoformat()}
                        }
                    }
                ],
                "should": [{
                    "term": {"beat.name": "search"}
                },
                    {
                        "term": {"beat.name": "eventlog"}
                    }
                ]
            }
        }
    }

    ES.indices.refresh(indexname)
    results = ES.update_by_query(index=indexname, body=searchbody,
                                 conflicts='proceed', wait_for_completion='true')  # search_timeout='1m')
    # results = ES.search(index=indexname, body=searchbody)
    ES.indices.refresh(indexname)
    # print ip, results["hits"]["total"]
    print(results)

    return results


def updaterecord(indexname, record, doctype="logevent"):
    ES.update(index=indexname,
              id=record["_id"],
              doc_type=doctype,
              body={"doc": record["_source"]}
              )


def process_new_events(indexname, new_events, live_sessions, sessionType):
    for record in new_events["hits"]["hits"]:

        # check for records that failed to parse in logstash
        # and assign a sessionid of -1. This is uncommon.
        recordtags = record["_source"].get("tags")
        if ("_jsonparsefailure" in recordtags
            or "_geoip_lookup_failure" in recordtags):
            record["_source"][sessionType] = -1
            updaterecord(indexname, record)
            continue

        # grab the timestamp and ip of the new event
        timestamp = record["_source"].get("@timestamp")
        clientip = record["_source"]["geoip"].get("ip")

        # check to see if this event is historic
        # if it is, then clear the sessionids of later events
        lastentrydate = get_last_processed_event_datetime_by_ip(indexname, clientip, sessionType)
        if lastentrydate is not None:
            # print lastentrydate, dateparser.parse(timestamp)
            if (lastentrydate > dateparser.parse(timestamp)):
                print('found events after', timestamp, 'for', clientip)
                remove_stale_sessionids(indexname, clientip, dateparser.parse(timestamp), sessionType)
                print('after update', get_last_processed_event_datetime_by_ip(indexname, clientip, sessionType))
                # continue

        # try to get session info from the live_sessions list
        session = live_sessions.get(clientip)

        # if no session is found, create a new session
        if session is None:
            live_sessions[clientip] = {}
            live_sessions[clientip][sessionType] = next(SESSION_ID)
            live_sessions[clientip]["timestamp"] = timestamp
            session = live_sessions.get(clientip)

        # check the session timestamp to see if ttl expired before current event
        delta = dateparser.parse(timestamp) - dateparser.parse(session["timestamp"])
        if (sessionType == "sessionid"):
            if ((delta.total_seconds() / 60) > TTL_MINUTES):
                live_sessions[clientip][sessionType] = next(SESSION_ID)
        if (sessionType == "hourlySessionId"):
            if ((delta.total_seconds() / 60) > TTL_MINUTES60):
                live_sessions[clientip][sessionType] = next(SESSION_ID)

        # update the session timestamp and id
        session["timestamp"] = timestamp
        record["_source"][sessionType] = session[sessionType]

        request = record["_source"].get("request", "")
        if request.startswith("/cn/v2/query/solr/"):
            record["_source"]["searchevent"] = True

        # print clientip, session

        # update the elasticsearch document with the session id
        updaterecord(indexname, record)

    return


def get_new_events(indexname, sessionType, batchsize=10000):
    searchbody = {
        "from": 0, "size": batchsize,
        "query": {
            "bool": {
                "must": {
                    "term": {"_type": "logevent"}
                },
                "should": [{
                    "term": {"beat.name": "search"}
                },
                    {
                        "term": {"beat.name": "eventlog"}
                    }
                ],
                "must_not": {
                    "exists": {
                        "field": sessionType
                    }
                }
            }
        },
        "sort": [{
            "@timestamp": {
                "order": "asc",
                "unmapped_type": "date"
            }
        }]
    }

    results = ES.search(index=indexname, body=searchbody)
    if not results["hits"]["hits"]:
        return None

    return results


def get_live_sessions_before_mark(indexname, mark, sessionType):
    # Find sessions that have not expired as of the mark

    live_sessions = {}

    searchbody = get_live_sessions_searchbody(mark, sessionType)

    results = ES.search(index=indexname, body=searchbody)

    for item in results["aggregations"]["group"]["buckets"]:
        record = item["group_docs"]["hits"]["hits"][0]["_source"]

        timestamp = record.get("@timestamp")
        clientip = record["geoip"].get("ip")
        if (sessionType == "sessionid"):
            sessionid = record.get(sessionType)
        if (sessionType == "hourlySessionId"):
            hourlySessionId = record.get(sessionType)

        live_sessions[clientip] = {}
        live_sessions[clientip]["timestamp"] = timestamp
        if (sessionType == "sessionid"):
            live_sessions[clientip][sessionType] = sessionid
        if (sessionType == "hourlySessionId"):
            live_sessions[clientip][sessionType] = hourlySessionId

    return live_sessions


def get_live_sessions_searchbody(mark, sessionType):
    # Construct the search body for
    # get_live_sessions_before_mark
    # based upon the time given in the mark
    # minus the TTL_MINUTES / TTL_MINUTES60  value

    searchbody = {
        "from": 0, "size": 0,
        "query": {
            "bool": {
                "must": {
                    "term": {"_type": "logevent"}
                },
                "should": [{
                    "term": {"beat.name": "search"}
                },
                    {
                        "term": {"beat.name": "eventlog"}
                    }
                ],
                "filter": {
                    "range": {
                        "@timestamp": {
                            "gte": "",
                            "lt": ""
                        }
                    }
                }
            }
        },
        "aggs": {
            "group": {
                "terms": {
                    "field": "geoip.ip"
                },
                "aggs": {
                    "group_docs": {
                        "top_hits": {
                            "size": 1,
                            "sort": [{
                                "@timestamp": {
                                    "order": "desc",
                                    "unmapped_type": "date"
                                }
                            }],
                            "_source": {"includes": ["@timestamp", "geoip.ip", sessionType]}
                        }
                    }
                }
            }
        }
    }

    # Fill in the from (gte) and to (lt) time range for the query
    if (sessionType == "sessionid"):
        gte = mark.isoformat() + "||-" + str(TTL_MINUTES) + "m"
        lt = mark.isoformat()
    elif (sessionType == "hourlySessionId"):
        gte = mark.isoformat() + "||-" + str(TTL_MINUTES60) + "m"
        lt = mark.isoformat()
    else:
        pass
    searchbody["query"]["bool"]["filter"]["range"]["@timestamp"]["gte"] = gte
    searchbody["query"]["bool"]["filter"]["range"]["@timestamp"]["lt"] = lt

    return searchbody


def get_first_unprocessed_event_datetime(indexname, sessionType):
    # In the given index, find the chronologically earliest
    # search or eventlog event that has no sessionid and
    # return the datetime of that event in UTC

    searchbody = {
        "from": 0, "size": 0,
        "query": {
            "bool": {
                "must": {
                    "term": {"_type": "logevent"}
                },
                "should": [{
                    "term": {"beat.name": "search"}
                },
                    {
                        "term": {"beat.name": "eventlog"}
                    }
                ],
                "must_not": {
                    "exists": {
                        "field": sessionType
                    }
                }
            }
        },
        "aggs": {
            "min_timestamp": {
                "min": {
                    "field": "@timestamp"
                }
            }
        }
    }

    try:
        results = ES.search(index=indexname, body=searchbody)
        esvalue = results["aggregations"]["min_timestamp"]["value"] or None
        # print results["hits"]["hits"][0]["_source"]["geoip"]["ip"]
        if not esvalue:
            return None
        mark = datetime.fromtimestamp(esvalue / 1000, tz=tzutc())
    except Exception as e:
        print(e)
        return None
    else:
        return mark


def get_count_unprocessed_event(indexname, sessionType):
    # In the given index, count the search or
    # eventlog events that have no sessionid
    searchbody = {
        "from": 0, "size": 0,
        "query": {
            "bool": {
                "must": {
                    "term": {"_type": "logevent"}
                },
                "should": [{
                    "term": {"beat.name": "search"}
                },
                    {
                        "term": {"beat.name": "eventlog"}
                    }
                ],
                "must_not": {
                    "exists": {
                        "field": sessionType
                    }
                }
            }
        }
    }

    try:
        results = ES.search(index=indexname, body=searchbody)
        count = results["hits"]["total"]
        if not count:
            return None
    except Exception as e:
        print(e)
        return None
    else:
        return count


def get_last_processed_event_datetime_by_ip(indexname, ip, sessionType):
    searchbody = {
        "from": 0, "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {"_type": "logevent"}
                    },
                    {
                        "exists": {
                            "field": sessionType
                        }
                    },
                    {
                        "range": {
                            sessionType: {"gt": 0}
                        }
                    },
                    {
                        "term": {"geoip.ip": ip}
                    }
                ],
                "should": [{
                    "term": {"beat.name": "search"}
                },
                    {
                        "term": {"beat.name": "eventlog"}
                    }
                ]
            }
        },
        "aggs": {
            "max_timestamp": {
                "max": {
                    "field": "@timestamp"
                }
            }
        }
    }

    try:
        results = ES.search(index=indexname, body=searchbody)
        esvalue = results["aggregations"]["max_timestamp"]["value"] or None
        if not esvalue:
            return None
        # print results["hits"]["hits"][0]["_source"]
        mark = datetime.fromtimestamp(esvalue / 1000, tz=tzutc())
    except Exception as e:
        print(e)
        return None
    else:
        return mark


def generate_next_sessionid(sessionType, indexname=None):
    # Get the next session id value.
    # The first time the generator is called, it gets
    # (the largest session id from elasticsearch) + 1,
    # after that it increments by 1 each time
    #
    # sessionid is stored as long in ES with a max
    # value of (2^63)-1, so we don't need to worry
    # about overflow for a while

    searchbody = {
        "from": 0, "size": 0,
        "aggs": {
            "max_id": {
                "max": {
                    "field": sessionType
                }
            }
        }
    }

    # get top elasticsearch sessionid, increment and return
    results = ES.search(index=indexname, body=searchbody)
    esvalue = results["aggregations"]["max_id"]["value"] or 0
    next_sessionid = int(esvalue) + 1
    yield next_sessionid

    # increment current session id and return
    while True:
        next_sessionid += 1
        yield next_sessionid


def delete_index(indexname):
    try:
        ES.indices.delete(indexname)
    except Exception as e:
        print(e)
        return 1

    return 0


def init_index(indexname, sessionType):
    # Create an index including specific attributes
    # in order to assign their data types
    indexconfig = {
        "mappings": {
            "apacheLine": {
                "properties": {
                    "sessionid": {"type": "long"},
                    "searchevent": {"type": "boolean"}
                }
            }
        }
    }

    try:
        ES.indices.create(indexname, indexconfig)
    except Exception as e:
        print(e)
        return 1
    return 0


def parse_args(args):
    helpdescription = """
  This program supports DataONE log aggregation for search and
  download in elasticsearch. 
  """

    parser = argparse.ArgumentParser(description=helpdescription)
    parser.add_argument('--indexname', action='store', required=True,
                        help='name of the elasticsearch index to be altered')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('--delete', action='store_true',
                       help='delete the elasticsearch index and exit -- '
                            'WARNING: DESTRUCTIVE: DELETES ALL LOG DATA')
    group.add_argument('--init', action='store_true',
                       help='initialize the elasticsearch index and exit')
    group.add_argument('--process', action='store_true',
                       help='process new log data within the index and exit')

    return parser.parse_args()


if __name__ == "__main__":
    SESSION_ID = generate_next_sessionid("hourlySessionId")
    main(sys.argv)

