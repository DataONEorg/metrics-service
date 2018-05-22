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
from pytz import timezone

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

  def __init__(self, config_file=None):
    self._L = logging.getLogger(self.__class__.__name__)
    self._es = None
    self._config = DEFAULT_ELASTIC_CONFIG
    if not config_file is None:
      self.loadConfig(config_file)


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
    config = configparser.ConfigParser()
    self._L.debug("Loading configuration from %s", config_file)
    config.read(config_file)
    for key, value in iter(self._config.items()):
      self._config[key] = config.get(CONFIG_ELASTIC_SECTION, key, fallback=value)
    self._config["port"] = int(self._config["port"])
    return self._config


  def connect(self, force_reconnect=False):
    if self._es is not None and not force_reconnect:
      self._L.info("Elastic Search connection already established.")
      return
    settings = {"host": self._config["host"],
                "port": self._config["port"],
                }
    self._es = Elasticsearch([settings,])


  def getInfo(self, show_mappings=False):
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


  def _getQueryTemplate(self, fields=None, date_start=None, date_end=None):
    search_body = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {"_type": "logevent"}
            },
            {
              "term": { "beat.name": "eventlog" }
            },
            {
              "term": { "formatType": "data" }
            },
            {
              "exists": { "field": "sessionid" }
            }
          ]
        }
      }
    }
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
      index = self._config["index"]
    search_body = self._getQueryTemplate(fields=fields, date_start=date_start, date_end=date_end)
    search_body["query"]["bool"]["must"].append({"term": { "event": event_type }})
    if not session_id is None:
      sessionid_search = {"term": {"sessionid": session_id}}
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
      index = self._config["index"]
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
      sessionid_search = {"term": {"sessionid": session_id}}
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
      index = self._config["index"]
    search_body = self._getQueryTemplate(date_start=date_start, date_end=date_end)
    search_body["size"] = 0 #don't return any hits
    if event_type is not None:
      search_body["query"]["bool"]["must"].append({"term": { "event": event_type }})
    aggregate_name = "group_by_session"
    aggregations =  {aggregate_name: {
                        "terms": {
                          "field":"sessionid",                            #group by session id
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


  def getDOIs(self, index=None, q=None, session_id=None, limit=10, date_start=None, date_end=None):
    '''
    This method scans the logs in the Elastic Search for records with PIDs
    :param index:
    :param q:
    :param session_id:
    :param limit:
    :param date_start:
    :param date_end:
    :return: Returns a List of all the dois in the ES.
    '''
    if index is None:
      index = self._config["index"]
    search_body = {
      "query": {
        "prefix": {
          "pid": "doi"
        }
      }
    }
    self._L.info("Executing: %s", json.dumps(search_body, indent=2))
    results = self._scan(query=search_body, index=index)
    res = []
    prefixes = []
    count = 0
    for hit in results:
      res.append(hit[0]["_source"]["pid"])
      prefixes.append(hit[0]["_source"]["pid"][4:11])
    dois = set(res)
    pref = set(prefixes)
    return dois, pref


  def getCitations(self):
    '''
    Gets citations from the crossref end point
    :return:
    '''
    runGetDOIs = True
    if(runGetDOIs):
      self.connect()
      dois, pref =  self.getDOIs()
    else:
      pref = {'10.6073', '10.5065', '10.1873', '10.5072', '10.1594'}

    for i in pref:
      res = requests.get("https://api.eventdata.crossref.org/v1/events/scholix?source=crossref&obj-id.prefix="+i)
      dict = res.json()
      for i in dict["message"]["link-packages"]:
        if ("doi:" + i["Target"]["Identifier"]["ID"]) in dois:
          print("Is in ? True. - " +  "doi:" + i["Target"]["Identifier"]["ID"])
        else:
          print("Is in ? False. - " + "doi:" + i["Target"]["Identifier"]["ID"])


