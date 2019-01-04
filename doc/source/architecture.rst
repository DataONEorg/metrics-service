Log Processing Architecture
===========================

Component Diagram
-----------------

.. uml::
    
    top to bottom direction
    
    ' For component diagram help see http://plantuml.com/component-diagram
    ' Define the components
    
    component comp1 [
    MN + CN events
    ]


    frame "Coordinating Node" as "cnserv" <<Server>> {
        frame "d1-processing" as "logagg" {
            [Log Aggregation] <<Service>>
        }
        
        database "logsolr" {
        }
        
        folder "Event Logs" as logs{
            component [JSON Log File] <<log>>
        }
        
        frame "Apache Logs" as "apache" {
            component [Apache Log File] as cn.logs <<log>>
        }
        
        [filebeat] as cn.filebeat <<Service>>

        [Log Aggregation] -down-> logs: "Events written to\nJSON log files"
        [Log Aggregation] -right-> logsolr: "Events stored\nin logsolr index"
        logs --> [cn.filebeat]:  ""
        apache -->  [cn.filebeat] : ""
    }

    frame "search.dataone.org" as "search" <<Server>> {
        frame "Apache Logs" as "search.apache" {
            component [Apache Log File] as search.logs <<log>>
        }
        [filebeat] as search.filebeat <<Service>>
        search.logs --> [search.filebeat]:  ""
    }
    
    frame "Log Proc Server" as "logproc" <<Server>> {
        component [Log Stash] <<ELK Stack Service>>
        
        component [Elastic Search] <<ELK Stack Service>>
    }

    component comp2 [
    Metrics Service
    ]
    
    
    ' Define the interactions
    comp1 --> [Log Aggregation]: ""
    TCP_5705 .. [Log Stash] : ""
    [cn.filebeat] ..> TCP_5705 : ""
    [search.filebeat] ..> TCP_5705 : ""
    [Log Stash] --> [Log Stash] : "Filtering and\nSession Calculation"
    [Log Stash] --> [Elastic Search] : ""
    comp2 -right-> [Elastic Search] : "Query for the log records"

General flow of event information in the DataONE system. Events occur at Member
and Coordinating Nodes and are aggregated by the Log Aggregation process that 
runs within the ``d1-processing`` application. Events are written to both a 
solr index and to JSON format log files. The log files are monitored by 
``filebeat`` and forwarded to a ``logstash`` service. The ``logstash`` service 
applies geocoding, robots identification, and user agent matching before adding 
to an ``elasticseach`` instance.

