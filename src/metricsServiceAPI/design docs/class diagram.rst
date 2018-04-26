Metrics Service Class Diagram
==============================

- The Falcon `metrics_service.py` is where the Falcon API creates a WSGI application and the Gunicorn server runs this application.

- Resource object is created from the class `` which is eventually mapped to the end-points into the `metrics_service.py`.

- The JSON `MetricsRequest` object from the HTTP requests and pass it on to the `MetricsHandler` class for further processing.

- The `MetricsHandler` class parses the `MetricsRequest` object and based on the filtering properties calls the
appropriate method of the `ApplicationDAO` class.

- The `MetricsDAO` class takes the connection object from the `DBConnectivity` class and creates a cursor that can
perform the `DBMS CRUD` operations. This class has methods that would query the Database tables / Materialized views.

- The `MetricsDAO` class retrieves the queries results from the Database and returns the results to the `MetricsHandler`
class. The `MetricsHandler` class forms a `MetricsResponse` object which is sent back to the client as HTTP Response.


Class Diagram
-----------------
..
  @startuml ./metrics-service-class-diagram.png

    !include ./plantuml-styles.txt

    top to bottom direction

    ' For class diagram help see http://plantuml.com/class-diagram
    ' Define the classes

    class DBConnectivity {
        + connection
        + get_connection()
    }

    class MetricsDAO {
        + connection
        + cursor
        + results
        + get_landing_page_metrics()
        + get_user_profile_metrics()
        + get_user_profile_charts()
        + get_search_metrics()
        + get_metrics()
        + upsert_metrics()
    }

    class MetricsHandler {
        + metricsRequest
        + metricsResponse
        + processRequest()
    }

    class MetricsReader {
        + on_get()
        + on_post()
    }
    
    class MetricsHarvester {
        + query_ES()
        + write_sessions()
    }
    
    class SessionManager {
        + generate_session_ids()
    }
    
    class ReportHandler {
        + query_SOLR()
        + generate_reports()
        + send_reports()
    }

    ' Define the interactions
    Client -down-> metrics_service :"HTTP_Request"
    metrics_service -up-> Client: "HTTP_Response"
    metrics_service -down-> MetricsReader: "GET_Request / POST_Request"
    MetricsReader -up-> metrics_service: ""GET_Request / POST_Request"
    metrics_service -down-> MetricsHarvester: "Update"
    MetricsReader -down-> MetricsHandler: "Process_Metrics_Request"
    MetricsHandler -up-> MetricsReader: "Response"
    MetricsHandler -down-> MetricsDAO: "Query"
    MetricsDAO -up-> MetricsHandler: "Results"
    DBConnectivity -right-> MetricsDAO: "Connection"
    MetricsHarvester -down-> MetricsDAO: "Create / Read / Update"
    MetricsDAO -up-> MetricsHarvester: "Results"
    SessionManager -left-> MetricsHarvester: "Session IDs"
    MetricsHarvester -left-> ReportHandler: "Records"
    ReportHandler -up-> Hub : "Reports"



  @enduml

.. image:: ./metrics-service-class-diagram.png

