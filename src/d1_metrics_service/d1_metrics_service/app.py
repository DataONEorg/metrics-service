"""
This code creates your WSGI application and aliases it as api.

We use `application` variable name since that is what Gunicorn,
by default, expects it to be called!

We create unique resource and map to its endpoint.
"""


import falcon

from .metricsreader import MetricsReader
from .servicehandler import ServiceHandler

api = application = falcon.API() # pylint: disable=invalid-name

# Creating a unique GET resource from the GetMetrics class
metrics_handler_resource = MetricsReader() # pylint: disable=invalid-name

service_handler_resource = ServiceHandler()

# Mapping the HTTP GET endpoint with its unique resource
api.add_route('/metrics', metrics_handler_resource)

# Mapping the HTTP POST endpoint with its unique resource
api.add_route('/metrics/filters', metrics_handler_resource)

# Setting GET endpoint for the service handler to start the automation service
api.add_route('/start', service_handler_resource)

# Setting GET endpoint for the service handler to stop the automation service
api.add_route('/stop', service_handler_resource)

# Setting GET endpoint for the service handler to restart the automation service
api.add_route('/restart', service_handler_resource)

# Setting GET endpoint for the service handler to get the status of automation service
api.add_route('/status', service_handler_resource)
