"""
Application Data Access Object module
"""
from .dbconnectivity import DBConnectivity

class ApplicationDAO:
    """
    This class contains various methods that query the
    database tables and the database materialized views.
    It uses the conneciton object of the DBConenctivity class.
    """

    def __init__(self):
        db = DBConnectivity()
        self.connection = db.get_connection()
        self.cursor = self.connection.cursor()

    def get_landing_page_metrics(self, request):
        """
        Method that queries the DB materialized views
        for the dataset landing page.
        :return: List of results
        """
        results = {}

        # executing the query
        self.cursor.execute("select * from landingpage3 where dataset_id in (\'" + "\',\'".join(request) + "\') "\
                                + "group by month, year, metrics_name, sum, dataset_id order by month, year;")

        # retrieving the results
        res = self.cursor.fetchall()

        # appending the results to a list and
        # returning it to the MetricsHandler class
        for items in res:
            if items[1] in results:
                results[items[1]].append(str(items[4]))
            else:
                results[items[1]] = []
                results[items[1]].append(str(items[4]))

            if 'Months' in results:
                if str(items[2])+"-"+str(items[3]) in results['Months']:
                    pass
                else:
                    results['Months'].append(str(items[2])+"-"+str(items[3]))
            else:
                results['Months'] = []
                results['Months'].append(str(items[2])+"-"+str(items[3]))



        return results

