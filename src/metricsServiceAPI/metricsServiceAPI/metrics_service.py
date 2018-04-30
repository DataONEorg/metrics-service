"""
This code creates your WSGI application and aliases it as api.

We use `application` variable name since that is what Gunicorn,
by default, expects it to be called!

We create unique resource and map to its endpoint.
"""


import falcon

from .metrics_handler import MetricsHandler

api = application = falcon.API() # pylint: disable=invalid-name

# Creating a unique GET resource from the GetMetrics class
metrics_handler_resource = MetricsHandler() # pylint: disable=invalid-name

# Mapping the HTTP GET endpoint with its unique resource
api.add_route('/metrics', metrics_handler_resource)

# Mapping the HTTP POST endpoint with its unique resource
api.add_route('/metrics/filter', metrics_handler_resource)
