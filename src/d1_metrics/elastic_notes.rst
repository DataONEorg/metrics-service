Setup and Operation of Elasticsearch Event Index
================================================

Log events travel a long path to get into the elasticsearch index:

1. MN or CN event
2. Log aggregation process running on a CN collects event
3. Log aggregation processes event, augmenting and pushing into solr
4. Python script copies events from solr index to log files on disk
5. Filebeat watches log files, sends entries to logstash
6. Logstash adjusts the event for elasticsearch and inserts to elasticsearch index
7. Python script computes sessions and adds to elasticsearch
8. Events are aggregated to produce reports

Minimal example report: https://docs.google.com/spreadsheets/d/1BUGzbYh24siVivMZxy634IcSqw1fVIjjqFf-ohLGd5Q/edit#gid=0

UI Mockup: https://projects.invisionapp.com/share/7MFRHSL6WZT#/screens/278315639_LandingPage

Step 4. Form of log events
--------------------------

Log events are pulled from the solr event core and placed into a text log file. Each event is serialized as a json
object, and occupies one line of the log file.

Pulling events from solr event core::

  # test to see how many event will be pulled
  d1_logaggwatch.py -f /var/log/dataone/logagg_mirror/d1logagg.log -l -l -t

  # retrieve the events and place them in the log file. Log files are rotated at about 1GB
  d1_logaggwatch.py -f /var/log/dataone/logagg_mirror/d1logagg.log -l -l


Example of a single event log line (rendered on multiple lines for readability)::

    {
      "_version_": 1594950652431171600,
      "versionCompliance": "v1",
      "id": "urn:node:mnTestNCEI.505187",
      "entryId": "505187",
      "nodeId": "urn:node:mnTestNCEI",
      "pid": "urn:uuid:6a5d440f-9fb5-4dae-9b3e-fd36b76f0b4f",
      "formatType": "METADATA",
      "formatId": "eml://ecoinformatics.org/eml-2.1.1",
      "dateLogged": "2018-03-13T19:49:38.416Z",
      "dateAggregated": "2018-03-14T21:37:52.547Z",
      "dateUpdated": "1900-01-01T00:00:00Z",
      "event": "read",
      "ipAddress": "128.111.54.76",
      "isRepeatVisit": false,
      "inPartialRobotList": true,
      "inFullRobotList": true,
      "isPublic": false,
      "size": 5483,
      "rightsHolder": "CN=arctic-data-admins,DC=dataone,DC=org",
      "subject": "CN=urn:node:cnStageUCSB1,DC=dataone,DC=org",
      "readPermission": [
        "CN=arctic-data-admins,DC=dataone,DC=org",
        "CN=arctic-data-admins,DC=dataone,DC=org"
      ],
      "userAgent": "Apache-HttpClient/4.3.6 (java 1.5)",
      "location": "34.4329, -119.8371",
      "country": "United States",
      "region": "California",
      "city": "Santa Barbara",
      "geohash_1": "9",
      "geohash_2": "9q",
      "geohash_3": "9q4",
      "geohash_4": "9q4g",
      "geohash_5": "9q4gc",
      "geohash_6": "9q4gch",
      "geohash_7": "9q4gch3",
      "geohash_8": "9q4gch36",
      "geohash_9": "9q4gch361"
    }


Step 5. Filebeat
----------------

File beat watches log files on the CNs and ships changes to logstash. Filebeat is fairly smart and will balance the
rate of sending with the receiver's backlog. We use the non-standard port of 5705 since it is open between the firewalls
at the UNM, UTK, and UCSB locations.

::

  filebeat.prospectors:
  - input_type: log
    paths: /var/log/dataone/logagg_mirror/d1logagg.log

  name: "eventlog"                                         # This is name of each event sent to logstash

  fields:
    env: "production"                                      # Statically set the value of the "env" field

  output.logstash:
    hosts: ["logstash.domain.name:5705"]
    bulk_max_size: 2048


.. Note::

   To reset the filebeat starting point, stop filebeat, then delete ``/var/lib/filebeat/registry`` and ``meta.json``
   before restarting.


Step 6. Logstash Processing
---------------------------

Logstash documentation: https://www.elastic.co/guide/en/logstash/current/plugins-outputs-elasticsearch.html

Logstash configuration files: ``/etc/logstash/conf.d``

Eventlog logstash pipeline::

    input {
    beats {
      port => 5705
    }
    }
    filter {
    if [beat][name] == "eventlog" {
      json {
        # remove the message property since this is duplicate information
        source => "message"
        remove_field => ["message"]
      }
      date {
        #set the timestamp of the event to be the dateLogged
        match => ["dateLogged","ISO8601"]
        target => "@timestamp"
      }
      geoip {
        source => "ipAddress"
        tag_on_failure => ["_geoip_lookup_failure"]
        remove_field => ["location","country","region","city"]
      }
      mutate {
        id => "eventmutation"
      }
    }
    }
    output {
    #output to the local elasticsearch instance
    if [beat][name] == "eventlog" {
        elasticsearch {
          hosts => ["127.0.0.1:9200"]
          index => "eventlog-0"
        }
    }
    }


Templates are described at: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-templates.html

Template for eventlog* documents::

  PUT _template/template_eventlog
  {
    "index_patterns": ["eventlog*"],
    "settings": {
      "number_of_shards": "1"
    },
    "mappings": {
      "doc": {
        "properties": {
          "event_type": {"type":"keyword"},
          "versionCompliance": {"type":"keyword"},
          "entryId":  {"type":"text"},
          "id": {"type":"text"},
          "nodeId": {"type":"keyword"},
          "pid":{"type":"text", "fields":{"key":{"type":"keyword"}}}
          "formatType": {"type":"keyword"},
          "formatId": {"type":"text", "fields":{"key":{"type":"keyword"}}},
          "size": {"type":"long"},
          "event": {"type":"text", "fields":{"key":{"type":"keyword"}}},
          "ipAddress": {"type":"ip"},
          "dateLogged": {"type":"date"},
          "dateUpdated": {"type":"date"},
          "dateAggregated": {"type":"date"},
          "userAgent": {"type":"text"},
          "isRepeatVisit": {"type":"boolean"},
          "inPartialRobotList": {"type":"boolean"},
          "inFullRobotList": {"type":"boolean"},
          "location": {"type":"geo_point"},
          "country": {"type":"text", "fields":{"key":{"type":"keyword"}}},
          "region": {"type":"text", "fields":{"key":{"type":"keyword"}}},
          "city": {"type":"text", "fields":{"key":{"type":"keyword"}}},
          "geohash_1": {"type":"geo_point", "fields":{"key":{"type":"keyword"}}},
          "geohash_2": {"type":"geo_point", "fields":{"key":{"type":"keyword"}}},
          "geohash_3": {"type":"geo_point", "fields":{"key":{"type":"keyword"}}},
          "geohash_4": {"type":"geo_point", "fields":{"key":{"type":"keyword"}}},
          "geohash_5": {"type":"geo_point", "fields":{"key":{"type":"keyword"}}},
          "geohash_6": {"type":"geo_point", "fields":{"key":{"type":"keyword"}}},
          "geohash_7": {"type":"geo_point", "fields":{"key":{"type":"keyword"}}},
          "geohash_8": {"type":"geo_point", "fields":{"key":{"type":"keyword"}}},
          "geohash_9": {"type":"geo_point", "fields":{"key":{"type":"keyword"}}},
          "rightsHolder": {"type":"text", "fields":{"key":{"type":"keyword"}}},
          "isPublic": {"type":"boolean"},
          "subject": {"type":"text", "fields":{"key":{"type":"keyword"}}},
          "readPermission":  {"type":"keyword"},
          "sessionId": {"type":"long"}
        }
      }
    }
  }

Note that the ``sessionId`` property is not present in eventlog entries until after Step 7 where sessions are calculated.


Step 7. Computing Sessions
--------------------------

TBD.


Example Operations
------------------

The following provides example queries that may be exectued using the kibana interface (or using ``curl`` from the
commandline).

Show everything query
.....................

Query everything in the eventlog index::

  GET /eventlog-*/_search


Any event records
.................

Event records are identified with the property `beat.name`::

  GET /eventlog-*/_search
  {
    "query": {
      "bool": {
        "must": [
          {
            "term": {"beat.name": "eventlog"}
          }
        ]
      }
    }
  }


Production environment ``read`` event records
.............................................

Filebeat adds the ``env`` field static value when reading the log files on the server::

  GET /eventlog-*/_search
  {
    "query": {
      "bool": {
        "must": [
          {
            "term": {"beat.name": "eventlog"}
          },
          {
            "term": {"fields.env": "production"}
          },
          {
            "term": {"event.key": "read"}
          }
        ]
      }
    }
  }


Production metadata ``read`` event records with ``sessionId``
.............................................................

After sessions are computed, each record will have a sessionId associated with it. Metadata ``read`` events are
considered to be views of the record::

  GET /eventlog-*/_search
  {
    "query": {
      "bool": {
        "must": [
          {
            "term": {"beat.name": "eventlog"}
          },
          {
            "term": {"fields.env": "production"}
          },
          {
            "term": {"event.key": "read"}
          },
          {
            "term": {"formatType": "METADATA"}
          },
          {
            "exists": {"field": "sessionId"}
          }
        ]
      }
    }
  }


Get events from month
.....................

Get events from the month of May 2018 using `date range query <daterangequery>_`::

  GET /eventlog-*/_search
  {
    "query": {
      "range": {
        "dateLogged": {
          "gte": "2018-05-01||/M",
          "lt": "2018-06-01||/M"
        }
      }
    }
  }

.. _daterangequery: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html



Get **total read events for each PID for the month** of May 2018. This requires paging of the results. To do so, start with::

  GET /eventlog-*/_search
  {
    "query": {
      "bool":{
        "must":[{
          "range": {
          "dateLogged": {
            "gte": "2018-05-01||/M",
            "lt": "2018-06-01||/M"
            }
          }
        },
        {
          "term":{"event.key":"read"}
        }
      ]
      }
    },
    "size":0,
    "track_total_hits": false,
    "aggs":{
      "pid_list": {
        "composite": {
          "size": 10000,
          "sources": [
            { "pid": { "terms": {"field":"pid.key"}}}
          ]
        }
      }
    }
  }

then for the next page of 10,000, use the ``pid`` of the last item retrieved for the ``after`` parameter::

  GET /eventlog-*/_search
  {
    "query": {
      "bool":{
        "must":[{
          "range": {
          "dateLogged": {
            "gte": "2018-05-01||/M",
            "lt": "2018-06-01||/M"
            }
          }
        },
        {
          "term":{"event.key":"read"}
        }
      ]
      }
    },
    "size":0,
    "track_total_hits": false,
    "aggs":{
      "pid_list": {
        "composite": {
          "size": 10000,
          "sources": [
            { "pid": { "terms": {"field":"pid.key"}}}
          ],
          "after":{"pid":"88ba351b2833f4fd12514ac1fdf8d4c1"}
      }
    }
  }

where ``88ba351b2833f4fd12514ac1fdf8d4c1`` is the pid value of the last entry in the previous page.


Get **metrics for a PID grouped by metric type, month, and year**::

    GET /eventlog-0/_search
    {
      "query": {
              "term": {
                "pid.key": "cbfs.127.22"
              }
      },
      "size": 0,
      "aggs": {
        "group_by_metric" :{
          "terms": {
            "field": "metric_type.key"
          },
          "aggs": {
            "group_by_month": {
              "date_histogram": {
                "field": "dateLogged",
                "interval": "month"
              },
              "aggs": {
                "group_by_day": {
                  "date_histogram": {
                    "field": "dateLogged",
                    "interval":"day"
                  }
                }
              }
            }
          }
        }
      }
    }

See https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html

Not that the above does not work yet because there's no ``metric_type`` in the index (will be after re-processing) and
the current events are only for a month of activity. Instead the below shows similar structure, except aggregating at
month and day levels::

    GET /eventlog-*/_search
    {
      "query": {
              "term": {
                "pid.key": "cbfs.127.22"
              }
      },
      "size": 0,
      "aggs": {
        "group_by_month": {
          "date_histogram": {
            "field": "dateLogged",
            "interval": "month"
          },
          "aggs": {
            "group_by_day": {
              "date_histogram": {
                "field": "dateLogged",
                "interval":"day"
              }
            }
          }
        }
      }
    }
