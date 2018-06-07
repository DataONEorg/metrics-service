'''
Implements a wrapper for the metrics Elastic Search service.
'''
import logging
import configparser
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import requests
import json
import datetime
from dateutil import parser as dateparser
from dateutil.tz import tzutc
from pytz import timezone
import json
import pprint

CONFIG_ELASTIC_SECTION = "elasticsearch"
DEFAULT_ELASTIC_CONFIG = {
  "host":"localhost",
  "port":9200,
  "index":"eventlog-0",
  }

class MetricsElasticSearch(object):
  '''

  '''

  MAX_AGGREGATIONS = 999999999 #get them all...
  BATCH_SIZE = 1000
  SESSION_TTL_MINUTES = 60
  F_SESSIONID = "sessionId"


  def __init__(self, config_file=None, index_name=None):
    self._L = logging.getLogger(self.__class__.__name__)
    self._es = None
    self._config = DEFAULT_ELASTIC_CONFIG
    if not config_file is None:
      self.loadConfig(config_file)
    self._session_id = None
    self._doc_type = "doc"
    self._beatname = "eventlog"
    if index_name is not None:
      self._L.info("Overriding index name with %s", index_name)
      self._config["index"] = index_name


  def _scan(self, query=None, scroll='5m', raise_on_error=True,
           preserve_order=False, size=1000, request_timeout=None, clear_scroll=True,
           scroll_kwargs=None, **kwargs):
    """
    COPIED FROM: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/helpers/__init__.py
    Modified to return number of results and to work with simple aggregations listing

    Simple abstraction on top of the
    :meth:`~elasticsearch.Elasticsearch.scroll` api - a simple iterator that
    yields all hits as returned by underlining scroll requests.
    By default scan does not return results in any pre-determined order. To
    have a standard order in the returned documents (either by score or
    explicit sort definition) when scrolling, use ``preserve_order=True``. This
    may be an expensive operation and will negate the performance benefits of
    using ``scan``.
    :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
    :arg query: body for the :meth:`~elasticsearch.Elasticsearch.search` api
    :arg scroll: Specify how long a consistent view of the index should be
        maintained for scrolled search
    :arg raise_on_error: raises an exception (``ScanError``) if an error is
        encountered (some shards fail to execute). By default we raise.
    :arg preserve_order: don't set the ``search_type`` to ``scan`` - this will
        cause the scroll to paginate with preserving the order. Note that this
        can be an extremely expensive operation and can easily lead to
        unpredictable results, use with caution.
    :arg size: size (per shard) of the batch send at each iteration.
    :arg request_timeout: explicit timeout for each call to ``scan``
    :arg clear_scroll: explicitly calls delete on the scroll id via the clear
        scroll API at the end of the method on completion or error, defaults
        to true.
    :arg scroll_kwargs: additional kwargs to be passed to
        :meth:`~elasticsearch.Elasticsearch.scroll`
    Any additional keyword arguments will be passed to the initial
    :meth:`~elasticsearch.Elasticsearch.search` call::
        scan(es,
            query={"query": {"match": {"title": "python"}}},
            index="orders-*",
            doc_type="books"
        )
    """
    scroll_kwargs = scroll_kwargs or {}

    if not preserve_order:
      query = query.copy() if query else {}
      query["sort"] = "_doc"
    # initial search
    resp = self._es.search(body=query, scroll=scroll, size=size,
                         request_timeout=request_timeout, **kwargs)
    total_hits = resp["hits"]["total"]
    logging.info("Total hits = %d", total_hits)

    scroll_id = resp.get('_scroll_id')
    if scroll_id is None:
      return

    current_entry = 0
    try:
      first_run = True
      while True:
        # if we didn't set search_type to scan initial search contains data
        if first_run:
          first_run = False
        else:
          resp = self._es.scroll(scroll_id, scroll=scroll,
                               request_timeout=request_timeout,
                               **scroll_kwargs)
        for hit in resp['hits']['hits']:
          current_entry += 1
          yield (hit, current_entry, total_hits)

        # check if we have any errrors
        if resp["_shards"]["successful"] < resp["_shards"]["total"]:
          self._L.warning(
            'Scroll request has only succeeded on %d shards out of %d.',
            resp['_shards']['successful'], resp['_shards']['total']
          )
          if raise_on_error:
            raise helpers.ScanError(
              scroll_id,
              'Scroll request has only succeeded on %d shards out of %d.' %
              (resp['_shards']['successful'], resp['_shards']['total'])
            )

        scroll_id = resp.get('_scroll_id')
        # end of scroll
        if scroll_id is None or not resp['hits']['hits']:
          break
    finally:
      if scroll_id and clear_scroll:
        self._es.clear_scroll(body={'scroll_id': [scroll_id]}, ignore=(404,))


  def loadConfig(self, config_file):
    '''
    Load configuration details from a YAML config file

    Args:
      config_file: path to a yaml config file

    Returns:
      loaded configuration information
    '''
    config = configparser.ConfigParser()
    self._L.debug("Loading configuration from %s", config_file)
    config.read(config_file)
    for key, value in iter(self._config.items()):
      self._config[key] = config.get(CONFIG_ELASTIC_SECTION, key, fallback=value)
    self._config["port"] = int(self._config["port"])
    return self._config


  @property
  def indexname(self):
    return self._config["index"]


  def connect(self, force_reconnect=False):
    '''
    Connect to the ElasticSearch server

    Args:
      force_reconnect: Force a reconnection even if one is already created

    Returns:
      nothing
    '''
    if self._es is not None and not force_reconnect:
      self._L.info("Elastic Search connection already established.")
      return
    settings = {"host": self._config["host"],
                "port": self._config["port"],
                }
    self._es = Elasticsearch([settings,])


  def getInfo(self, show_mappings=False):
    '''
    Get basic information about the Elastic Search instance.

    Args:
      show_mappings:

    Returns:

    '''
    res = {}
    res["info"] = self._es.info()
    indices = self._es.cat.indices(format="json")
    for index in indices:
      index_name = index["index"]
      if show_mappings:
        mapping = self._es.indices.get_mapping(index=index_name)
        index["mapping"] = mapping
    res["indices"] = indices
    return res


  def _getQueryTemplate(self,
                        fields=None,
                        date_start=None,
                        date_end=None,
                        formatTypes=None,
                        session_required=True):
    search_body = {
      "query": {
        "bool": {
          "must": [
            {
              "term": { "beat.name": self._beatname }
            },
          ]
        }
      }
    }
    if formatTypes is not None:
      entry = {"term": { "formatType": formatTypes } }
      search_body["query"]["bool"]["must"].append(entry)
    if session_required:
      entry = {"exists": {"field": MetricsElasticSearch.F_SESSIONID } }
      search_body["query"]["bool"]["must"].append(entry)
    if not fields is None:
      search_body["_source"] = fields
    date_filter = None
    if date_start is not None or date_end is not None:
      date_filter = {"range":{"dateLogged":{}}}
      if date_start is not None:
        date_filter["range"]["dateLogged"]["gt"] = date_start.isoformat()
      if date_end is not None:
        date_filter["range"]["dateLogged"]["lte"] = date_end.isoformat()
      search_body["query"]["bool"]["filter"] = date_filter
    return search_body


  def _getQueryResults(self, index, search_body, limit):
    self._L.info("Executing: %s", json.dumps(search_body, indent=2))
    results = self._scan(query=search_body, index=index)
    counter = 0
    total_hits = 0
    data = []
    for hit in results:
      result = hit[0]
      counter = hit[1]
      total_hits = hit[2]
      if counter > limit:
        break
      entry = {}
      entry["event_id"] = result["_id"]
      for k,v in iter(result["_source"].items()):
        entry[k] = v
      data.append(entry)
    return data, total_hits


  def getEvents(self,
                index=None,
                event_type="read",
                session_id=None,
                limit=10,
                date_start=None,
                date_end=None,
                fields=None):
    if index is None:
      index = self.indexname
    search_body = self._getQueryTemplate(fields=fields, date_start=date_start, date_end=date_end)
    search_body["query"]["bool"]["must"].append({"term": { "event": event_type }})
    if not session_id is None:
      sessionid_search = {"term": {MetricsElasticSearch.F_SESSIONID: session_id}}
      search_body["query"]["bool"]["must"].append(sessionid_search)
    return self._getQueryResults(index, search_body, limit)


  def getSearches(self,
                  index=None,
                  q=None,
                  session_id=None,
                  limit=10,
                  date_start=None,
                  date_end=None,
                  fields=None):
    if index is None:
      index = self.indexname
    search_body = self._getQueryTemplate(fields=fields, date_start=date_start, date_end=date_end)
    if q is None:
      q = {
        "query_string": {
          "default_field": "message",
          "query": "\/cn\/v2\/query\/solr\/",
        }
      }
    search_body["query"]["bool"]["must"].append( q )
    if not session_id is None:
      sessionid_search = {"term": {MetricsElasticSearch.F_SESSIONID: session_id}}
      search_body["query"]["bool"]["must"].append(sessionid_search)
    return self._getQueryResults(index, search_body, limit)


  def getSessions(self,
                  index=None,
                  event_type=None,
                  limit=10,
                  date_start=None,
                  date_end=None,
                  min_aggs=1):
    '''
    Retrieve a list of session + count for each session
    Args:
      index:
      event_type:
      limit:
      date_start:
      date_end:

    Returns:

    '''
    if index is None:
      index = self.indexname
    search_body = self._getQueryTemplate(date_start=date_start, date_end=date_end)
    search_body["size"] = 0 #don't return any hits
    if event_type is not None:
      search_body["query"]["bool"]["must"].append({"term": { "event": event_type }})
    aggregate_name = "group_by_session"
    aggregations =  {aggregate_name: {
                        "terms": {
                          "field":MetricsElasticSearch.F_SESSIONID,                            #group by session id
                          "size": MetricsElasticSearch.MAX_AGGREGATIONS,  #max number of groups to return
                          "exclude": [-1,]                                #exclude terms that match values in this array
                        },
                        "aggs": {                                           #compute the min and max dateLogged for each session
                          "min_date":{"min":{"field":"dateLogged"}},
                          "max_date": {"max": {"field": "dateLogged"}},
                        }
                      }
                    }
    if min_aggs is not None:
      aggregations[aggregate_name]["terms"]["min_doc_count"] = min_aggs
    search_body["aggs"] = aggregations
    self._L.info("Request: \n%s", json.dumps(search_body, indent=2))
    resp = self._es.search(body=search_body, request_timeout=None)
    total_hits = resp["hits"]["total"]
    logging.info("Total hits = %d", total_hits)
    results = []
    counter = 0
    naggregates = len(resp["aggregations"][aggregate_name]["buckets"])
    UTC = timezone('UTC')
    for bucket in resp["aggregations"][aggregate_name]["buckets"]:
      self._L.debug(json.dumps(bucket, indent=2))
      tmin = datetime.datetime.fromtimestamp(bucket["min_date"]["value"]/1000)
      tmin.replace(tzinfo=UTC)
      tmax = datetime.datetime.fromtimestamp(bucket["max_date"]["value"]/1000)
      tmax.replace(tzinfo=UTC)
      results.append([bucket["key"], bucket["doc_count"], tmin, tmax])
      if counter >= limit:
        return results, naggregates
      counter += 1
    return results, naggregates


  def countUnprocessedEvents(self, index_name=None):
    '''
    Count the number of events that have no sessionId.

    Args:
      index_name: name of the index containing events

    Returns:
      integer, number of events without a sessionId
    '''
    count = 0
    search_body = {
      "from": 0, "size": 0,
      "query": {
        "bool": {
          "must": [
            {
              "term": {"beat.name": self._beatname}
            },
            {
              "term": {"event.key": "read"}
            },
          ],
          "must_not": {
            "exists": {
              "field": MetricsElasticSearch.F_SESSIONID
            }
          }
        }
      }
    }
    if index_name is None:
      index_name = self.indexname
    try:
      results = self._es.search(index=index_name, body=search_body)
      count = results["hits"]["total"]
      if count is None:
        raise ValueError("no total hits!")
      return count
    except Exception as e:
      self._L.error(e)
    return 0


  def getNextSessionId(self, index_name=None):
    '''
    Generator for sessionIds.

    Gets the next available session id (long type)
    Args:
      index_name:

    Returns:
      long
    '''
    search_body = {
      "from": 0, "size": 0,
      "aggs": {
        "max_id": {
          "max": {
            "field": MetricsElasticSearch.F_SESSIONID
          }
        }
      }
    }
    if index_name is None:
      index_name = self.indexname
    results = self._es.search(index=index_name, body=search_body)
    last_session_id = results["aggregations"]["max_id"]["value"] or 0
    next_session_id = int(last_session_id)
    yield next_session_id
    while True:
      next_session_id += 1
      yield next_session_id


  def getFirstUnprocessedEventDatetime(self, index_name=None):
    search_body = {
      "from": 0, "size": 0,
      "query": {
        "bool": {
          "must": [
            {
              "term": {"beat.name": self._beatname}
            },
            {
              "term": {"event.key": "read"}
            },
          ],
          "must_not": {
            "exists": {
              "field": MetricsElasticSearch.F_SESSIONID
            }
          }
        }
      },
      "aggs": {
        "min_timestamp": {
          "min": {
            "field": "dateLogged"
          }
        }
      }
    }
    if index_name is None:
      index_name = self.indexname
    try:
      results = self._es.search(index=index_name, body=search_body)
      esvalue = results["aggregations"]["min_timestamp"]["value"] or None
      if esvalue is None:
        raise ValueError("No max dateLogged available!")
      mark = datetime.datetime.fromtimestamp(esvalue / 1000, tz=tzutc())
      return mark
    except Exception as e:
      self._L.error(e)
    return None


  def getLiveSessionsSearchBody(self, mark):
    search_body = {
      "from": 0, "size": 0,
      "query": {
        "bool": {
          "must": [
            {
              "term": {"beat.name": self._beatname}
            },
            {
              "term":{"event.key": "read"}
            },
          ],
          "filter": {
            "range": {
              "dateLogged": {
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
            "field": "ipAddress"
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
                "_source": {"includes": ["dateLogged", "ipAddress", MetricsElasticSearch.F_SESSIONID]}
              }
            }
          }
        }
      }
    }
    gte = mark.isoformat() + "||-" + str(MetricsElasticSearch.SESSION_TTL_MINUTES) + "m"
    lt = mark.isoformat()
    search_body["query"]["bool"]["filter"]["range"]["dateLogged"]["gte"] = gte
    search_body["query"]["bool"]["filter"]["range"]["dateLogged"]["lt"] = lt
    return search_body


  def getLiveSessionsBeforeMark(self, index_name=None, mark=None):
    live_sessions = {}
    if index_name is None:
      index_name = self.indexname
    search_body = self.getLiveSessionsSearchBody( mark )
    self._L.debug(json.dumps(search_body, indent=2))
    results = self._es.search(index=index_name, body=search_body)
    self._L.debug(str(results))
    for item in results["aggregations"]["group"]["buckets"]:
      record = item["group_docs"]["hits"]["hits"][0]["_source"]
      time_stamp = record.get("dateLoged")
      client_ip = record.get("ipAddress")
      session_id = record.get(MetricsElasticSearch.F_SESSIONID)
      live_sessions[client_ip] = {}
      live_sessions[client_ip]["timestamp"] = time_stamp
      live_sessions[client_ip][MetricsElasticSearch.F_SESSIONID] = session_id
    return live_sessions


  def getNewEvents(self, index_name=None, batch_size=BATCH_SIZE):
    if index_name is None:
      index_name = self.indexname
    search_body = {
      "from": 0, "size": batch_size,
      "query": {
        "bool": {
          "must": [
            {
              "term": {"beat.name": self._beatname}
            },
            {
              "term":{"event.key": "read"}
            },
          ],
          "must_not": {
            "exists": {
              "field": MetricsElasticSearch.F_SESSIONID
            }
          }
        }
      },
      "sort": [{
        "dateLogged": {
          "order": "asc",
          "unmapped_type": "date"
        }
      }]
    }
    try:
      results = self._es.search(index=index_name, body=search_body)
      if results["hits"]["hits"] is None:
        raise ValueError("No hits in result.")
      return results
    except Exception as e:
      self._L.error(e)
    return None


  def getLastProcessedEventDatetimeByIp(self, index_name=None, client_ip=None):
    if index_name is None:
      index_name = self.indexname
    search_body = {
      "from": 0, "size": 0,
      "query": {
        "bool": {
          "must": [
            {
              "term": {"beat.name": self._beatname}
            },
            {
              "term":{"event.key": "read"}
            },
            {
              "exists": {
                "field": MetricsElasticSearch.F_SESSIONID
              }
            },
            {
              "range": {
                "sessionId": {"gt": 0}
              }
            },
            {
              "term": {"ipAddress": client_ip}
            }
          ],
        }
      },
      "aggs": {
        "max_timestamp": {
          "max": {
            "field": "dateLogged"
          }
        }
      }
    }
    try:
      results = self._es.search(index=index_name, body=search_body)
      esvalue = results["aggregations"]["max_timestamp"]["value"] or None
      if esvalue is None:
        raise ValueError("No max_timestamp available!")
      mark = datetime.datetime.fromtimestamp(esvalue / 1000, tz=tzutc())
      return mark
    except Exception as e:
      self._L.debug(e)
    return None


  def removeStaleSessionIds(self, index_name=None, client_ip=None, time_stamp=None):
    if index_name is None:
      index_name = self.indexname
    search_body = {
      "script": {
        "inline": "ctx._source.remove(\"" + MetricsElasticSearch.F_SESSIONID + "\")",
        "lang": "painless"
      },
      "query": {
        "bool": {
          "must": [
            {
              "term": {"beat.name": self._beatname}
            },
            {
              "exists": {
                "field": MetricsElasticSearch.F_SESSIONID
              }
            },
            {
              "range": {
                MetricsElasticSearch.F_SESSIONID: {"gt": 0}
              }
            },
            {
              "term": {"ipAddress": client_ip}
            },
            {
              "range": {
                "@timestamp": {"gt": time_stamp.isoformat()}
              }
            }
          ]
        }
      }
    }
    self._es.indices.refresh(index_name)
    results = self._es.update_by_query(index=index_name,
                                       body=search_body,
                                       conflicts="proceed",
                                       wait_for_completion="true")
    self._es.indices.refresh(index_name)
    self._L.debug(results)
    return results


  def updateRecord(self, index_name, record):
    self._es.update(index=index_name,
                    id = record["_id"],
                    doc_type=self._doc_type,
                    body={"doc": record["_source"]})


  def _processNewEvents(self, index_name=None, new_events=[], live_sessions=[]):
    if index_name is None:
      index_name = self.indexname
    counter = 0
    total_count = len(new_events["hits"]["hits"])
    for record in new_events["hits"]["hits"]:
      counter += 1
      if counter % 100 == 0:
        self._L.info("%d / %d", counter, total_count)
      # check for records that failed to parse in logstash
      # and assign a sessionid of -1. This is uncommon.
      recordtags = record["_source"].get("tags")
      if ("_jsonparsefailure" in recordtags
          or "_geoip_lookup_failure" in recordtags):
        if "_jsonparsefailure" in recordtags:
          self._L.debug("_jsonparsefailure in recordtags")
        if "_geoip_lookup_failure" in recordtags:
          #pprint.pprint(record)
          self._L.debug("_geoip_lookup_failure in recordtags: %s", record["_source"]["ipAddress"])
        record["_source"][MetricsElasticSearch.F_SESSIONID] = -1
        self.updateRecord(index_name, record)
        self._L.debug("Event Session set to INVALID (-1)")
        continue
      timestamp = record["_source"].get("dateLogged")
      client_ip = record["_source"].get("ipAddress")

      last_entry_date = self.getLastProcessedEventDatetimeByIp(index_name, client_ip)
      if last_entry_date is not None:
        if last_entry_date > dateparser.parse(timestamp):
          self._L.warning("Found events after %s for %s",timestamp, client_ip)
          self.removeStaleSessionIds(index_name, client_ip, dateparser.parse(timestamp))
          self._L.warning("After update %s", self.getLastProcessedEventDatetimeByIp(index_name, client_ip))

      session = live_sessions.get(client_ip)
      if session is None:
        live_sessions[client_ip] = {}
        live_sessions[client_ip][MetricsElasticSearch.F_SESSIONID] = next(self._session_id)
        live_sessions[client_ip]["timestamp"] = timestamp
        session = live_sessions[client_ip]

      try:
        delta = dateparser.parse(timestamp) - dateparser.parse(session["timestamp"])
      except TypeError as e:
        self._L.error("Session structure: %s", pprint.pformat(session))
        self._L.error(e)
        raise(e)
      if ((delta.total_seconds() / 60 ) > MetricsElasticSearch.SESSION_TTL_MINUTES):
        live_sessions[client_ip][MetricsElasticSearch.F_SESSIONID] = next(self._session_id)

      session["timestamp"] = timestamp
      record["_source"][MetricsElasticSearch.F_SESSIONID] = session[MetricsElasticSearch.F_SESSIONID]

      request = record["_source"].get("request", "")
      if request.startswith("/cn/v2/query/solr/"):
        record["_source"]["searchevent"] = True
      self.updateRecord(index_name, record)



  def computeSessions(self,
                      index_name=None,
                      dry_run=False):
    es_logger = logging.getLogger('elasticsearch')
    es_logger.propagate = False
    es_logger.setLevel(logging.WARNING)
    if index_name is None:
      index_name = self.indexname
    self._session_id = self.getNextSessionId(index_name)
    batch_size = 1000
    batch_counter = 0
    self._es.indices.refresh(index_name)
    unprocessed_count = self.countUnprocessedEvents(index_name)
    self._L.info("Unprocessed events = %d", unprocessed_count)
    total_batches = unprocessed_count / batch_size + bool(unprocessed_count % batch_size)
    self._L.info("Number of batches = %d at %d per batch", total_batches, batch_size)
    if dry_run:
      return 0
    while True:
      self._es.indices.refresh(index_name)
      mark = self.getFirstUnprocessedEventDatetime(index_name)
      if mark is None:
        return 0
      self._L.info("At mark: %s", mark.isoformat())
      live_sessions = self.getLiveSessionsBeforeMark(index_name, mark)
      self._L.debug(json.dumps(live_sessions))
      new_events = self.getNewEvents(index_name, batch_size)
      self._processNewEvents(index_name, new_events, live_sessions)
      self._L.info("Processed batch %d of %d", batch_counter, total_batches)
      batch_counter += 1
    return 1


  def get_aggregations(self,
                  date_start,
                  date_end,
                  index=None,
                  query=None,
                  aggQuery=None,
                  after_record = None):
    '''
    Retrieve a response for aggregations
    Args:
      date_start:
      date_end:
      index:
      query:
      aggQuery:
      after_record:

    Returns: Aggregations dictionary

    '''
    if index is None:
      index = self.indexname
    search_body = {
      "size": 0,
      "query": {
        "bool": {
          "must": [

          ],
          "filter": {
            "range": {
              "dateLogged": {
                "gte": date_start.isoformat(),
                "lte": date_end.isoformat()
              }
            }
          }
        }
      },
      "aggs": {

      }
    }
    if(query is not None):
      search_body["query"]["bool"]["must"].append(query)
    if(aggQuery is not None):
      search_body["aggs"] = aggQuery
    if(after_record is not None):
      search_body["aggs"]["pid_list"]["composite"]["after"] = {}
      search_body["aggs"]["pid_list"]["composite"]["after"] = after_record
    self._L.info("Request: \n%s", json.dumps(search_body, indent=2))
    resp = self._es.search(body=search_body, request_timeout=None)
    return(resp)



  def iterate_composite_aggregations(self, start_date, end_date, search_query = None, aggregation_query = None):
    count = 0
    total = 0
    size = 100
    if(count == total == 0):
      aggregations = self.get_aggregations( query=search_query, aggQuery = aggregation_query, date_start=start_date, date_end=end_date)
      count = count + size
      total = aggregations["hits"]["total"]
    while( count < total):
      after = aggregations["aggregations"]["pid_list"]["buckets"][-1]
      temp = self.get_aggregations(query=search_query, aggQuery = aggregation_query, date_start=start_date, date_end=end_date, after_record=after["key"])
      if(len(temp["aggregations"]["pid_list"]["buckets"]) == 0):
        break
      aggregations["aggregations"]["pid_list"]["buckets"] = aggregations["aggregations"]["pid_list"]["buckets"] \
                                                            + temp["aggregations"]["pid_list"]["buckets"]
      count = count + size
    return aggregations


# if __name__ == "__main__":
#   md = MetricsElasticSearch()
# #   # md.get_report_header("01/20/2018", "02/20/2018")
#   md.connect()
# #   data = md.get_report_aggregations()
#   data = md.iterate_composite_aggregations()
