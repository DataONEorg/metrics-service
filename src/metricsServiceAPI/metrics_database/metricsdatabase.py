import sys
import os
import argparse
import logging
import psycopg2
import configparser

DEFAULT_CONFIG_FILE="/etc/dataone/metrics/database.ini"
CONFIG_DATABASE_SECTION = "database"
DEFAULT_DB_CONFIG = {
  "host":"localhost",
  "port":"5432",
  "dbname":"metrics",
  "user":"metrics_user",
  "password":""
  }


class MetricsDatabase(object):

  def __init__(self, config_file=None):
    self._L = logging.getLogger(self.__class__.__name__)
    self.conn = None
    self._config = DEFAULT_DB_CONFIG
    if not config_file is None:
      self.loadConfig(config_file)


  def loadConfig(self, config_file):
    config = configparser.ConfigParser()
    self._L.debug("Loading configuration from %s", config_file)
    config.read(config_file)
    for key, value in iter(self._config.items()):
      self._config[key] = config.get(CONFIG_DATABASE_SECTION, key, fallback=value)
    return self._config


  def connect(self, force_new=False):
    if not self.conn is None and not force_new:
      self._L.info("Connection to database already established.")
      return
    self._L.info("Connecting to {user}@{host}:{port}/{dbname}".format(**self._config))
    self.conn = psycopg2.connect(**self._config)


  def getCursor(self):
    return self.conn.cursor()


  def getSingleValue(self, csr, sql):
    self._L.debug("getSingleValue: %s", sql)
    csr.execute(sql)
    row = csr.fetchone()
    return row[0]


  def initializeDatabase(self, sql_files):
    with self.getCursor() as csr:
      for sql_file in sql_files:
        self._L.info("Loading: %s", sql_file)
        sql = open(sql_file, "r", encoding="utf8").read()
        self._L.debug("Executing: %s", sql)
        csr.execute(sql)
    self.conn.commit()


  def summaryReport(self):
    res = {}
    operations = {
      "version":"SELECT version FROM db_version;",
      "rows": "SELECT count(*) FROM metrics;",
      "landingpages":"SELECT count(*) FROM landingpage;",
      "userprofilemetrics": "SELECT count(*) FROM userprofilemetrics;",
      "userprofilecharts": "SELECT count(*) FROM userprofilecharts;",
      "repometrics": "SELECT count(*) FROM repometrics;",
      "repocharts": "SELECT count(*) FROM repocharts;",
      "awardmetrics": "SELECT count(*) FROM awardmetrics;",
      "awardcharts": "SELECT count(*) FROM awardcharts;",
    }
    with self.getCursor() as csr:
      for key,value in iter(operations.items()):
        res[key] = self.getSingleValue(csr, value)
    return res



