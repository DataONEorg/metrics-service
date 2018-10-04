'''
Implements a log aggregation record processor which reads records from the
solr aggregated log index, computes session information, and persists the
results to a log file as JSON records, with one record per line.
'''

import os
import logging
import logging.handlers
import json
import datetime
import dateutil
from file_read_backwards import FileReadBackwards
from d1_logagg import sessionid
from d1_logagg import sessionstates

ID_CONTEXT = ""
ID_ISSUER = "001"


class RecordProcessor(object):

  OUTPUT_LOG_NAME = "logagg"
  MAX_LOGFILE_SIZE = 1073741824  # 1GB
  MAX_LOG_BACKUPS = 250  # max of about 200GB of log files stored
  DEFAULT_LOG_FILE = "d1logagg_v2.log"
  DATE_LOGGED = "dateLogged"
  SESSION_DURATION = 60.0
  EVENT_TYPE = "read"

  def __init__(self,
               output_file=DEFAULT_LOG_FILE,
               id_context=ID_CONTEXT,
               id_issuer=ID_ISSUER,
               session_duration=SESSION_DURATION):
    self._output_file = output_file
    self.outlog = self._getOutputLogger(self._output_file)
    self.id_generator = sessionid.SessionId(context=id_context, issuer=id_issuer)
    self.session_states = sessionstates.SessionStates(session_duration, RecordProcessor.EVENT_TYPE, self.id_generator)


  def _getOutputLogger(this, log_file, log_level=logging.INFO):
    '''
    Logger used for emitting the solr records as JSON blobs, one record per line.

    Only really using logger for this to take advantage of the file rotation capability.

    Args:
      log_file: path of file to output, with rotation
      log_level: detail level at which logger runs at

    Returns:
      logger to emit messages to log_file
    '''
    logger = logging.Logger(name=RecordProcessor.OUTPUT_LOG_NAME)
    # Just emit the JSON
    formatter = logging.Formatter('%(message)s')
    l1 = logging.handlers.RotatingFileHandler(
      filename=log_file,
      mode='a',
      maxBytes=RecordProcessor.MAX_LOGFILE_SIZE,
      backupCount=RecordProcessor.MAX_LOG_BACKUPS
    )
    l1.setFormatter(formatter)
    l1.setLevel(log_level)
    logger.addHandler(l1)
    return logger


  def writeEvent(self, event):
    msg = json.dumps(event, indent=None)
    self.outlog.info(msg)


  def readEvent(self, event_file):
    entry = event_file.readline()
    if entry is None:
      return None
    while True:
      entry = entry.strip()
      if len(entry) > 0:
        record = json.loads(entry)
        if not isinstance(record[RecordProcessor.DATE_LOGGED], datetime.datetime):
          # convert to datetime
          # 2018-06-01T20:09:40.542Z
          record[RecordProcessor.DATE_LOGGED] = dateutil.parser.parse(record[RecordProcessor.DATE_LOGGED])
        return record
      entry = event_file.readline()


  def loadHistory(self, max_duration):
    '''
    Load historical records from persisted json log files in order to
    initialize the session state processors.

    Look a the last output message to get the latest date, then need to look
    backwards through the log files to find something with date older than
    the latest date + the session length. That guarantees the session states are
    appropriately initialized before processing.

    Args:
      max_duration: Age in minutes of the oldest record to retrieve

    Returns:
      List of historical records covering a time period of at least max_duration

    '''
    results = []
    if not os.path.exists( self._output_file ):
      return results
    latest_date = None
    with FileReadBackwards(self._output_file, encoding="utf-8") as source_file:
      for entry in source_file:
        entry = entry.strip()
        if len(entry) > 0:
          record = json.loads(entry)
          if not isinstance(record[RecordProcessor.DATE_LOGGED], datetime.datetime):
            # convert to datetime
            # 2018-06-01T20:09:40.542Z
            record[RecordProcessor.DATE_LOGGED] = dateutil.parser.parse(record[RecordProcessor.DATE_LOGGED])
          if latest_date is None:
            latest_date = record[RecordProcessor.DATE_LOGGED]
          delta = (latest_date - record[RecordProcessor.DATE_LOGGED]).total_seconds() / 60.0
          if delta > max_duration:
            break
          results.append(record)
          logging.debug(record)
    self.session_states.initializeSessionStates(results)
    return results


  def processSolr(self, event_sources):
    '''

    Args:
      event_sources: list of event files to read, in chronological order

    Returns:
      nothing
    '''
    pass