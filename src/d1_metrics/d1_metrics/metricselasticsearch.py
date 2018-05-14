'''
Implements a wrapper for the metrics Elastic Search service.
'''
import logging
import configparser
import collections
from elasticsearch import Elasticsearch
from elasticsearch import helpers

CONFIG_ELASTIC_SECTION = "elasticsearch"
DEFAULT_ELASTIC_CONFIG = {
  "host":"localhost",
  "port":9200,
  "index":"logstash-test0",
  }

class MetricsElasticSearch(object):
  '''

  '''

  def __init__(self, config_file=None):
    self._L = logging.getLogger(self.__class__.__name__)
    self._es = None
    self._config = DEFAULT_ELASTIC_CONFIG
    if not config_file is None:
      self.loadConfig(config_file)


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


  def _getQueryTemplate(self):
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
    return search_body



  def _getQueryResults(self, index, search_body, limit):
    results = helpers.scan(self._es, query=search_body, index=index)
    counter = 0
    data = []
    for result in results:
      counter += 1
      entry = {}
      entry["event_id"] = result["_id"]
      for k,v in iter(result["_source"].items()):
        entry[k] = v
      data.append(entry)
      if counter >= limit:
        break
    return data


  def getEvents(self, index=None, event_type="read", session_id=None, limit=10):
    if index is None:
      index = self._config["index"]
    search_body = self._getQueryTemplate()
    search_body["query"]["bool"]["must"].append({"term": { "event": event_type }})
    if not session_id is None:
      sessionid_search = {"term": {"sessionid": session_id}}
      search_body["query"]["bool"]["must"].append(sessionid_search)
    return self._getQueryResults(index, search_body, limit)


  def getSearches(self, index=None, q=None, session_id=None, limit=10):
    if index is None:
      index = self._config["index"]
    search_body = self._getQueryTemplate()
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

