"""
Metrics Handler module
"""

from .metricsDAO import ApplicationDAO

class MetricsHandler:
    """
    This class parses the metricsRequest obeject
    and based on the filters queries the appropriate
    method of the ApplicationDAO class.
    """

    def __init__(self, metricsRequest):
        self.request = metricsRequest
        self.response = self.request


    def process_request(self):
        """
        This method parses the filters of the
        MetricsRequest object
        :return:
        """
        application_dao = ApplicationDAO()
        metrics_page = self.request['metricsPage']
        filter_by = self.request['filterBy']
        metrics = self.request['metrics']
        group_by = self.request['groupBy']

        for items in filter_by:
            if items['filterType'] == "dataset" and items['interpretAs'] == "list":
                results = application_dao.get_landing_page_metrics(items['values'])
                self.response.update(results)

        return self.response
