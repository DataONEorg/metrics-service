MDC Log Processing Architecture
===============================

.. TODO: Describe the architecture of how the components interact

Component Diagram
-----------------
..
  @startuml mdc-log-processing-architecture.png

    !include plantuml-styles.txt
    
    left to right direction
    
    ' For component diagram help see http://plantuml.com/component-diagram
    ' Define the components
    
    component comp1 [
    MN
    +
    CN
    logs
    ]

    frame "CN Stage Server" as "cnserv" <<Server>> {
        frame "Log Aggregation" as "logagg" {
            [Log Aggregation Index] <<Service>>
        }
        
        folder "Script" as script{
            component [d1logagg.py] <<Script>>
        }
        
        folder "Logs" as logs{
            component [Log File] <<log>>
        }
        
        frame "Apache Logs" as "apache" {
            component [Apache Log File] <<log>>
        }

        
        [File Beat Stream] <<Service>>

    }
    
    frame "Log Proc Server" as "logproc" <<Server>> {
        component [Log Stash] <<ELK Stack Service>>
        
        component [Elastic Search] <<ELK Stack Service>>
        
        folder "Maintenance Script" as mainScript{
            component [Ed's index maintenance.py] <<Script>>
        }
        
        database "PostGresQL" {

        }
    }

    
    
    
    ' Define the interactions
    comp1 --> [Log Aggregation Index]: ""
    [Log Aggregation Index] -left-> script:  ""
    script -down-> logs: manual execution
    logs -down-> [File Beat Stream]:  ""
    apache -up->  [File Beat Stream] : ""
    [Log Stash] .up. TCP : ""
    TCP .up.> [File Beat Stream] : ""
    [Log Stash] --> [Log Stash] : ""
    [Log Stash] --> [Elastic Search] : ""
    [Elastic Search] --> mainScript : ""
    mainScript --> [Elastic Search] : ""
    [Elastic Search] --> PostGresQL : ""
    


    
  @enduml

.. image:: ./images/mdc-log-processing-architecture.png

