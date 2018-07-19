from d1_metrics_service.app import application
import schedule
import time
import datetime

# Handled by the user to start the process
flagAutomationProcesses = False

# Flag to check if this is the first WSGI call
isFirstWSGICall = False

if(isFirstWSGICall is False):
    isFirstWSGICall = True


    def job():
        file = open("testfile.txt", "w")

        file.write(datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y"), " : ", "Hello World")

        file.close()


        schedule.every(1).minutes.do(job)

    while(flagAutomationProcesses and isFirstWSGICall):
        schedule.run_pending()
        time.sleep(1)
else:
    pass