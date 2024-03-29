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
  "index":"eventlog-1",
  "request_timeout":60,
  }

class MetricsElasticSearch(object):
  '''

  '''

  MAX_AGGREGATIONS = 999999999  #get them all...
  BATCH_SIZE = 1000             # Size of a batch when computing sessions
  SESSION_TTL_MINUTES = 60      # Duration of a session
  F_SESSIONID = "sessionId"     # Name of the sessionId field
  F_DATELOGGED = "dateLogged"   # Name of the field where the event timestamp is recorded
  F_IPADDR = "ipAddress"        # name ofthe IP Address field


  def __init__(self, config_file=None, index_name=None):
    self._L = logging.getLogger(self.__class__.__name__)
    self._es = None
    self._config = DEFAULT_ELASTIC_CONFIG
    if not config_file is None:
      self.loadConfig(config_file)
    self._session_id = None
    self._doc_type = "doc"
    self._entryname = "eventlog"
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
    """
    Generates the query template to perform ES query
    :param fields:
    :param date_start:
    :param date_end:
    :param formatTypes:
    :param session_required:
    :return: Dictionary object for the search body based on the parameters passed.
    """
    search_body = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {"fields.entryType": self._entryname}
            },
          ],
          "must_not": [
            {
              "terms": {
                "tags" : [
                  "ignore_ip",
                  "machine_ua",
                  "robot_ua",
                  "dataone_ip",
                  "robot_ip",
                  "d1_admin_subject"
                ]
              }
            }
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
      date_filter = {"range":{MetricsElasticSearch.F_DATELOGGED:{}}}
      if date_start is not None:
        date_filter["range"][MetricsElasticSearch.F_DATELOGGED]["gt"] = date_start.isoformat()
      if date_end is not None:
        date_filter["range"][MetricsElasticSearch.F_DATELOGGED]["lte"] = date_end.isoformat()
      search_body["query"]["bool"]["filter"] = date_filter
    return search_body


  def _getQueryResults(self, index, search_body, limit, rawSearches=False, request_timeout=None):
    """

    :param index:
    :param search_body:
    :param limit:
    :param rawSearches:
    :return: Returns ES Query results
    """
    self._L.info("Executing: %s", json.dumps(search_body, indent=2))
    if request_timeout:
      results = self._scan(query=search_body, scroll='1h', index=index, request_timeout=request_timeout)
    else:
      results = self._scan(query=search_body, scroll='1h', index=index)

    rawSearchCounter = 0
    total_hits = 0
    data = []
    if rawSearches:

      for hit in results:
        rawSearchCounter += 1
        if rawSearchCounter > limit:
          break
        result = hit[0]
        counter = hit[1]
        total_hits = hit[2]

        data.append(result)
      return data, total_hits

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
    """
    Does a query to the ES given proper parameters and returns the result to the calling function.
    :param index:
    :param event_type:
    :param session_id:
    :param limit:
    :param date_start:
    :param date_end:
    :param fields:
    :return: Dictionary of results from the ES. Results are aggregated and includes complete paginated responses.
    """
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
    """
    Performs a search Query on the ES index. Based on the parametrs passes, it sets the search body and returns the
    result to the calling function.
    :param index:
    :param q:
    :param session_id:
    :param limit:
    :param date_start:
    :param date_end:
    :param fields:
    :return: Dictionary of results from the ES. Results are aggregated and includes complete paginated responses.
    """
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


  def getRawSearches(self,
                  index=None,
                  q=None,
                  must_not_q=None,
                  session_id=None,
                  limit=10,
                  date_start=None,
                  date_end=None,
                  fields=None,
                  request_timeout=30):
    """
    Performs a search Query on the ES index. Based on the parametrs passes, it sets the search body and returns the
    result to the calling function.
    :param index:
    :param q:
    :param session_id:
    :param limit:
    :param date_start:
    :param date_end:
    :param fields:
    :return: Dictionary of results from the ES. Results are aggregated and includes complete paginated responses.
    """
    if index is None:
      index = self.indexname

    # Set up a raw search query
    search_body = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {"event.key": "read"}
            },
            {
              "terms": {
                "formatType": [
                  "DATA",
                  "METADATA"
                ]
              }
            }
          ],
          "must_not": [
            {
              "terms": {
                "tags": [
                  "ignore_ip",
                  "machine_ua",
                  "robot_ua",
                  "dataone_ip",
                  "robot_ip",
                  "d1_admin_subject"
                ]
              }
            }
          ]
        }
      }
    }

    if q is None:
      q = {
        "query_string": {
          "default_field": "message",
          "query": "\/cn\/v2\/query\/solr\/",
        }
      }
    else:
      if (isinstance(q, list) ):
        for query_object in q:
          search_body["query"]["bool"]["must"].append(query_object)
      if (isinstance(q, dict) ):
        search_body["query"]["bool"]["must"].append(q)

    if must_not_q is not None:
      if (isinstance(must_not_q, dict)):
        search_body["query"]["bool"]["must_not"].append(must_not_q)

    self._L.info("query=" + json.dumps(search_body))
    try:
      self._L.info("retrieving events...")
      data = self._getQueryResults(index=index, search_body=search_body, limit=limit, rawSearches=True, request_timeout=30)
      return data

    except Exception as e:
      print("error")
      self._L.error(e)
    return None


  def updateEvents(self, index_name, record):
    """
    Updates the event in the ES index
    :param index_name:
    :param record:
    :return: Boolean flag for update status
    """
    try:
      self._es.update(index=index_name,
                    id = record["_id"],
                    doc_type=self._doc_type,
                    body={"doc": record["_source"]})

      return True
    except Exception as e:
      self._L.error(e)
    return False



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
                          "min_date":{"min":{"field":MetricsElasticSearch.F_DATELOGGED}},
                          "max_date": {"max": {"field": MetricsElasticSearch.F_DATELOGGED}},
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
              "term": {"fields.entryType": self._entryname}
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
    '''
    Returns the datetime of the oldest read event with no session information.

    Args:
      index_name: Name of index to use

    Returns:
      datetime or None
    '''
    search_body = {
      "from": 0, "size": 0,
      "query": {
        "bool": {
          "must": [
            {
              "term": {"fields.entryType": self._entryname}
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
            "field": MetricsElasticSearch.F_DATELOGGED
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
        raise ValueError("No unprocessed events.")
      mark = datetime.datetime.fromtimestamp(esvalue / 1000, tz=tzutc())
      return mark
    except Exception as e:
      self._L.warning(e)
    return None


  def getLiveSessionsSearchBody(self, mark):
    '''
    Returns a search body for retrieving active sessions.

    An active session is a session where the oldest read event was logged less than
    the duration of SESSION_TTL_MINUTES ago.

    There may be multiple sessions matching that critera, so the outcome is a list of
    [dateLogged, IPAddress, sessionId]

    Args:
      mark: datetime to measure from

    Returns: a search body for the query

    '''
    search_body = {
      "from": 0, "size": 0,
      "query": {
        "bool": {
          "must": [
            {
              "term": {"fields.entryType": self._entryname}
            },
            {
              "term":{"event.key": "read"}
            },
          ],
          "filter": {
            "range": {
              MetricsElasticSearch.F_DATELOGGED: {
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
            "field": MetricsElasticSearch.F_IPADDR
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
                "_source": {"includes": [
                  MetricsElasticSearch.F_DATELOGGED,
                  MetricsElasticSearch.F_IPADDR,
                  MetricsElasticSearch.F_SESSIONID
                  ]
                }
              }
            }
          }
        }
      }
    }
    #This says:  >= mark_timestamp - 60 minutes
    gte = mark.isoformat() + "||-" + str(MetricsElasticSearch.SESSION_TTL_MINUTES) + "m"
    lt = mark.isoformat()
    search_body["query"]["bool"]["filter"]["range"][MetricsElasticSearch.F_DATELOGGED]["gte"] = gte
    search_body["query"]["bool"]["filter"]["range"][MetricsElasticSearch.F_DATELOGGED]["lt"] = lt
    return search_body


  def getLiveSessionsBeforeMark(self, index_name=None, mark=None):
    '''
    Get a dictionary of sessions and timestamp indexed by IPaddress for active sessions.

    Args:
      index_name: Name of the event index
      mark: datetime to match for live sessions.

    Returns:
      {ip: {
        timestamp: timestamp of oldest event in session,
        sessionId: session
        }
      }
    '''
    live_sessions = {}
    if index_name is None:
      index_name = self.indexname
    search_body = self.getLiveSessionsSearchBody( mark )
    self._L.debug(json.dumps(search_body, indent=2))
    results = self._es.search(index=index_name, body=search_body)
    self._L.debug(str(results))
    for item in results["aggregations"]["group"]["buckets"]:
      record = item["group_docs"]["hits"]["hits"][0]["_source"]
      time_stamp = record.get(MetricsElasticSearch.F_DATELOGGED)
      client_ip = record.get(MetricsElasticSearch.F_IPADDR)
      session_id = record.get(MetricsElasticSearch.F_SESSIONID)
      if client_ip in live_sessions:
        self._L.warning("Multiple live sessions detected for a single IP address! : %s", client_ip)
      #TODO: what's the best course of action here? Bail? Override?, Ignore?
      live_sessions[client_ip] = {}
      live_sessions[client_ip]["timestamp"] = time_stamp
      live_sessions[client_ip][MetricsElasticSearch.F_SESSIONID] = session_id
    return live_sessions


  def getNewEvents(self, index_name=None, batch_size=BATCH_SIZE):
    '''
    Get a batch of events that are not associated with a session
    Args:
      index_name: Name of the index to use
      batch_size: Maximum number of records to return in the batch

    Returns: elastic search response structure with the events, ordered by date logged.
    '''
    if index_name is None:
      index_name = self.indexname
    search_body = {
      "from": 0, "size": batch_size,
      "query": {
        "bool": {
          "must": [
            {
              "term": {"fields.entryType": self._entryname}
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
        MetricsElasticSearch.F_DATELOGGED: {
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
              "term": {"fields.entryType": self._entryname}
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
              "term": {MetricsElasticSearch.F_IPADDR: client_ip}
            }
          ],
        }
      },
      "aggs": {
        "max_timestamp": {
          "max": {
            "field": MetricsElasticSearch.F_DATELOGGED
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
              "term": {"fields.entryType": self._entryname}
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
              "term": {MetricsElasticSearch.F_IPADDR: client_ip}
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
                                       request_timeout=self._config["request_timeout"],
                                       wait_for_completion="true")
    self._es.indices.refresh(index_name)
    self._L.debug(results)
    return results


  def updateRecord(self, index_name, record):
    self._es.update(index=index_name,
                    id = record["_id"],
                    doc_type=self._doc_type,
                    request_timeout=self._config["request_timeout"],
                    body={"doc": record["_source"]})


  def _processNewEvents(self, index_name=None, new_events=[], live_sessions=[]):
    '''
    Update new_events with sessionIds from either existing sessions or new sessions.

    This is where sessionIds get assigned and new sessions get created.

    Args:
      index_name: name of the event index
      new_events:  List of events for which session is to be calculated
      live_sessions: The list of active sessions

    Returns: nothing

    '''
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
        #skip further processing on this record and get the next one
        continue
      timestamp = record["_source"].get(MetricsElasticSearch.F_DATELOGGED)
      client_ip = record["_source"].get(MetricsElasticSearch.F_IPADDR)

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
    '''
    Updates event records with session a session id.

    Args:
      index_name: Name of the index to use
      dry_run: If True, then just show work to be done

    Returns: 0 if ok.
    '''
    #Quiet down the elastic search logger a bit
    es_logger = logging.getLogger('elasticsearch')
    es_logger.propagate = False
    es_logger.setLevel(logging.WARNING)
    if index_name is None:
      index_name = self.indexname

    #Get the next session id to be assigned
    self._session_id = self.getNextSessionId(index_name)
    batch_size = MetricsElasticSearch.BATCH_SIZE
    batch_counter = 0
    self._es.indices.refresh(index_name)

    # Determine how much work to do
    unprocessed_count = self.countUnprocessedEvents(index_name)
    self._L.info("Unprocessed events = %d", unprocessed_count)
    total_batches = unprocessed_count / batch_size + bool(unprocessed_count % batch_size)
    self._L.info("Number of batches = %d at %d per batch", total_batches, batch_size)
    if dry_run:
      return 0
    # keep working until the work is completed
    while True:
      # refresh any index caching so the event list reflects any additions or updates
      self._es.indices.refresh(index_name)

      # find the first event with no session info computed
      # mark is a datetime
      mark = self.getFirstUnprocessedEventDatetime(index_name)
      if mark is None:
        self._L.info("Completed computeSessions.")
        return 0
      self._L.info("At mark: %s", mark.isoformat())

      # get a list of the active sessions
      live_sessions = self.getLiveSessionsBeforeMark(index_name, mark)
      self._L.debug(json.dumps(live_sessions))

      # get a batch of events to process
      new_events = self.getNewEvents(index_name, batch_size)

      # Process the events by assigning them to existing sessions or starting new sessions.
      self._processNewEvents(index_name=index_name, new_events=new_events, live_sessions=live_sessions)

      batch_counter += 1
      self._L.info("Processed batch %d of %d", batch_counter, total_batches)
    return 1


  def get_aggregations(self,
                  date_start,
                  date_end,
                  index=None,
                  query=None,
                  aggQuery=None,
                  after_record = None):
    """
    Retrieve a response for aggregations
    :param date_start:
    :param date_end:
    :param index:
    :param query:
    :param aggQuery:
    :param after_record:
    :return: Aggregations dictionary
    """

    if index is None:
      index = self.indexname
    search_body = {
      "size": 0,
      "query": {
        "bool": {
          "must": [
          ],
          "must_not": [
            {
              "terms": {
                "tags": [
                  "ignore_ip",
                  "machine_ua",
                  "robot_ua",
                  "dataone_ip",
                  "robot_ip",
                  "d1_admin_subject"
                ]
              }
            }
          ],
          "filter": {
            "range": {
              MetricsElasticSearch.F_DATELOGGED: {
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
    self._L.debug("Request: %s", str(search_body))
    resp = self._es.search(body=search_body, request_timeout=self._config["request_timeout"])
    return(resp)



  def iterate_composite_aggregations(self, start_date, end_date, search_query = None, aggregation_query = None):
    """
    Performs pagination using the `after` parameter of the Composite aggregations of the ES.
    :param start_date:
    :param end_date:
    :param search_query:
    :param aggregation_query:
    :return: Returns aggregated list of all the results retrieved from the ES
    """
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



  def getDatasetIdentifierFamily(self, search_query, index="identifiers-2", max_limit=10):
    """
    Based on the search_query, query the ES to get the Dataset Identifier Family
    from the ES `identifiers` index
    :return:
    """
    search_body = {}
    counter = max_limit
    search_body["_source"] = ["PID", "datasetIdentifierFamily"]
    search_body["query"] = search_query
    return self._getQueryResults(index, search_body, max_limit)


# if __name__ == "__main__":
#   md = MetricsElasticSearch()
# #   # md.get_report_header("01/20/2018", "02/20/2018")
#   md.connect()
# #   data = md.get_report_aggregations()
#   data = md.iterate_composite_aggregations()
