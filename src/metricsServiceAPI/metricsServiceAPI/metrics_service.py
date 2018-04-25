"""
This code creates your WSGI application and aliases it as api.

We use `application` variable name since that is what Gunicorn,
by default, expects it to be called!

We create unique resource and map to its endpoint.
"""


import falcon

from .getMetrics import GetMetrics
from .postmetricsfilter import PostMetricsFilters

api = application = falcon.API() # pylint: disable=invalid-name

# Creating a unique GET resource from the GetMetrics class
getmetricsrequest = GetMetrics() # pylint: disable=invalid-name

# Mapping the HTTP GET endpoint with its unique resource
api.add_route('/getMetrics', getmetricsrequest)


# Creating a unique POST resource from the PostMetricsFilters class
postmetricsrequest = PostMetricsFilters() # pylint: disable=invalid-name

# Mapping the HTTP POST endpoint with its unique resource
api.add_route('/metrics/filter', postmetricsrequest)
