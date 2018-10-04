MDC Log Processing Architecture
===============================

.. TODO: Describe the architecture of how the components interact

Component Diagram
-----------------
..
  @startuml mdc-log-processing-architecture.png


    !include plantuml-styles.txt

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
        
        folder "Logs" as logs{
            component [JSON Log File] <<log>>
        }
        
        frame "Apache Logs" as "apache" {
            component [Apache Log File] as cn.logs <<log>>
        }
        
        [filebeat] as cn.filebeat <<Service>>
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
    [Log Aggregation] -down-> logs: "Events written to\nJSON log files [1]"
    [Log Aggregation] -right-> logsolr: "Events stored\nin logsolr index"
    logs --> [cn.filebeat]:  ""
    apache -->  [cn.filebeat] : ""
    TCP_5705 .. [Log Stash] : ""
    [cn.filebeat] ..> TCP_5705 : ""
    [Log Stash] --> [Log Stash] : "Filtering and\nSession Calculation[2]"
    [Log Stash] --> [Elastic Search] : ""
    comp2 -right-> [Elastic Search] : "Query for the log records"


