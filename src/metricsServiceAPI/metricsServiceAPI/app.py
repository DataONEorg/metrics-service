import falcon

from .getMetrics import GetMetrics

api = application = falcon.API()

getmetrics = GetMetrics()
api.add_route('/getMetrics', getmetrics)