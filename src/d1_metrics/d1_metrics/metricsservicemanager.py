'''
Implements the automation services
'''


import sys
import time
import schedule
from datetime import datetime
from daemon import Daemon
from d1_metrics.metricsreporter import MetricsReporter
from d1_metrics.metricsdatabase import  MetricsDatabase



class MetricsServiceManager(object):

    def __init__(self):
        pass

    def job():
        with open("test.txt", "a") as myfile:
            myfile.write("I'm still working at")
            myfile.write(datetime.now().strftime('%m/%d/%Y'))
        print("I'm working...")

    def run(self):
        metrics_database = MetricsDatabase()
        metrics_reporter = MetricsReporter()
        schedule.every(1).minute.do(job_func=self.job())
        schedule.every().hour.do(job_func=self.job())
        schedule.every().day.at("00:30").do(job_func=self.job())
        # schedule.every().day.at("01:30").do(job_func=metrics_reporter.scheduler())
        # schedule.every().day.at("02:30").do(job_func=metrics_database.getCitations())
        while True:
            schedule.run_pending()
            time.sleep(1)


class MyDaemon(Daemon):
    def run(self):
        service_manager_object = MetricsServiceManager()
        service_manager_object.run()


if __name__ == "__main__":
    # daemon = MyDaemon('/tmp/daemon-example.pid')
    # if len(sys.argv) == 2:
    #     if 'start' == sys.argv[1]:
    #         daemon.start()
    #     elif 'stop' == sys.argv[1]:
    #         daemon.stop()
    #     elif 'restart' == sys.argv[1]:
    #         daemon.restart()
    #     else:
    #         print("Unknown command")
    #         sys.exit(2)
    #     sys.exit(0)
    # else:
    #     print("usage: %s start|stop|restart" % sys.argv[0])
    #     sys.exit(2)
    service_manager_object = MetricsServiceManager()
    service_manager_object.run()