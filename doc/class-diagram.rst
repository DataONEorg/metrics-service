Metrics Service Class Diagram
==============================

- The Falcon `d1_metrics_service` class is where the Falcon API creates a WSGI application and the Gunicorn server runs this application.

- Resource object is created from the class `MetricsReader` which is eventually mapped to the end-points into the `d1_metrics_service`.

- The JSON `MetricsRequest` object from the HTTP requests is passed on to the `MetricsReader` class for further processing.

- The `MetricsReader` class parses the `MetricsRequest` object and based on the filtering properties calls the appropriate method of the `MetricsDatabase` class from the `d1_metrics` package.

- The `MetricsDatabase` class has methods that establishes connection and returns a cursor that could perform `CRUD` operations on the database. This class also has multiple methods that helps interact with the PostgreSQL database. 

- The `MetricsDatabase` class retrieves the queries results from the Database and returns the results to the `MetricsReader` class. The `MetricsReader` class forms a `MetricsResponse` object which is sent back to the client as HTTP Response.

- The `MetricsReporter` class queries `solr` to get the metadata for the generation of reports on a scheduled basis. These reports are send to the hub on month-to-month basis.

- The `MetricsElasticSearch` class has methods to retrieve information from the Elastic Search.


Class Diagram
-----------------
..
  @startuml ./images/metrics-service-class-diagram.png

    !include ./plantuml-styles.txt

    skinparam linetype ortho
    left to right direction

    ' For class diagram help see http://plantuml.com/class-diagram
    ' Define the classes

    package d1_metrics {
        class MetricsDatabase {
            + loadConfig()
            + connect()
            + getCursor()
            - _iterRow()
            + getSingleValue()
            + initializeDatabase()
            + summaryReport()
            + setMetadataValue()
            + getMetadataValue()
            + deleteMetadataValue()
            + getMetadata()
            + getSummaryMetricsPerDataset()
            + getSummaryMetricsPerUser()
            + getMetricsPerUser()
            + getSummaryMetrics()
            + getMetrics()
            + upsertMetrics()
        }
        
        note bottom of MetricsDatabase
            Interacts with the PostgreSQL database
        end note
        
        class MetricsReporter {
            + querySolr()
            + generateReports()
            + sendReports()
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
            + processRequest()
            + on_get()
            + on_post()
        }

        note bottom of MetricsReader
            Responds to REST requests with
            JSON results from the database
        end note

        class d1_metrics_service {

        }
        
        note bottom of d1_metrics_service
            Provides the REST interface for
            client metric queries using Falcon
        end note

    }

    ' Define the interactions
    d1_metrics_service -down- MetricsReader: requests > 
    MetricsReader -up- MetricsDatabase: reads >
    MetricsElasticSearch -down- MetricsDatabase: updates >
    MetricsReporter -down- MetricsDatabase: reads >


  @enduml

.. image:: ./images/metrics-service-class-diagram.png

