"""
This code creates your WSGI application and aliases it as api.

We use `application` variable name since that is what Gunicorn,
by default, expects it to be called!

We create unique resource and map to its endpoint.
"""


import falcon
import logging

from d1_metrics_service.metricsreader import MetricsReader
from d1_metrics_service.citationsmanager import CitationsManager

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)18s: %(message)s'
                    )

api = application = falcon.API() # pylint: disable=invalid-name

# Creating a resource handler for the Falcon API that handles the HTTP requests
metrics_handler_resource = MetricsReader() # pylint: disable=invalid-name
citations_manager_resource = CitationsManager() # pylint: disable=invalid-name

# Mapping the HTTP endpoint with its unique resource.
# Used for both the GET and the POST endpoints
api.add_route('/metrics', metrics_handler_resource)
api.add_route('/metrics/filters', metrics_handler_resource)

# Mapping the Citations HTTP endpoint with its unique resource.
# Used for both the GET and the POST endpoints
api.add_route('/citations', citations_manager_resource)


