Example Elasticsearch Queries
=============================

The service can be accessed at https://logproc-stage-ucsb-1.test.dataone.org/app/kibana

Elasticsearch can be accessed locally by opening a ssh tunnel::

  ssh -L9200:localhost:9200 logproc-stage-ucsb-1.test.dataone.org


Metrics Service
---------------

Landing Page Query Request
..........................

.. literalinclude:: includes/es_queries/LandingPageQueryRequest.json
   :language: json



User Profile Charts
...................

.. literalinclude:: includes/es_queries/UserProfileCharts.json
   :language: json


User Profile Summary
....................

.. literalinclude:: includes/es_queries/UserProfileSummary.json
   :language: json


Event Processing
----------------

Unprocessed events
..................

.. literalinclude:: includes/es_queries/unprocessed_events.json
   :language: javascript

