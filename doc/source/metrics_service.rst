Metrics Service Class Diagram
==============================

- The Falcon `d1_metrics_service` class is where the Falcon API creates a WSGI application and the Gunicorn server runs this application.

- Resource object is created from the class `MetricsReader` which is eventually mapped to the end-points into the `d1_metrics_service`.

- The JSON `MetricsRequest` object from the HTTP requests is passed on to the `MetricsReader` class for further processing.

- The `MetricsReader` class parses the `MetricsRequest` object and based on the filtering properties calls the appropriate method of the `MetricsElasticSearch` and the `MetricsDatabase` class from the `d1_metrics` package.

- The `MetricsElasticSearch` class has various methods to aggregate the metrics based on the filters specified in the `MetricsReader` class. The `MetricsReader` class forms a `MetricsResponse` object which is sent back to the client as HTTP Response.

- The `MetricsDatabase` class has methods that establishes connection and returns a cursor that could perform `CRUD` operations on the Citations database. This class is reponsible for gathering Citations for the DataONE objects from the Crossref Citations API and updating the metadata on timely basis.

- The `MetricsReporter` class queries `solr` to get the metadata for the generation of reports on a scheduled basis. These reports are send to the hub on month-to-month basis.



Class Diagram
-----------------
..
  @startuml ../images/metrics-service-class-diagram.png

    !include ../plantuml-styles.txt

    skinparam linetype ortho
    left to right direction

    ' For class diagram help see http://plantuml.com/class-diagram
    ' Define the classes

.. uml::

    package d1_metrics {
        class MetricsDatabase {
            + loadConfig()
            + connect()
            + getCursor()
            + getDOIs()
            + getCitations()
            + updateCitationMetadata()
            + getSingleValue()
            + initializeDatabase()
            - _iterRow()
        }
        
        note bottom of MetricsDatabase
            Interacts with the PostgreSQL database to manage DataONE Citations
        end note
        
        class MetricsReporter {
            + report_handler()
            + get_report_header()
            + get_unique_pids()
            + generate_instances()
            + get_report_datasets()
            + resolve_MN()
            + send_reports()
            + query_solr()
            + scheduler()
        }
        
        note bottom of MetricsReporter
            MetricsReporter sends reports to the
            DataCite Tech Hub on a scheduled 
            basis. Querys DataONE Solr Search 
            Core on the fly.
        end note
        
        class MetricsElasticSearch {
            + getEvents()
            + getSearches()
            + loadConfig()
            + connect()
            + getInfo()
            + setSessionId()
            + get_aggregations()
            + iterate_composite_aggregations()
            + computeSessions()
            - _getQueryTemplate()
            - _getQueryResults()
        }
        
        note bottom of MetricsElasticSearch
            Interacts with the Elastic Search index
        end note

    }

    package d1_metrics_service {
        class MetricsReader {
            + metricsRequest
            + metricsResponse
            + on_get()
            + on_post()
            + processRequest()
            + getSummaryMetricsPerDataset()
            + gatherCitations()
            + parseResponse()
            + formatData()
            + resolvePIDs()
        }

        note bottom of MetricsReader
            Responds to REST requests with
            JSON results from the ES index and the database
        end note

        class d1_metrics_service {

        }
        
        note bottom of d1_metrics_service
            Provides the REST interface for
            client metric queries using Falcon
        end note

    }

    ' Define the interactions
    d1_metrics_service -left- MetricsReader: requests >
    MetricsReader -- MetricsElasticSearch: reads >
    MetricsReader -- MetricsDatabase: reads >
    MetricsReporter -right- MetricsElasticSearch: reads >


