import logging
import psycopg2
import configparser
import collections
try:
  from cPickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
except ImportError:
  from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL


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


  def _iterRow(self, cursor, num_rows=100):
    while True:
      rows = cursor.fetchmany(num_rows)
      if not rows:
        break
      for row in rows:
        yield row


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
    res = collections.OrderedDict()
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
    res["metadata"] = self.getMetadata()
    return res


  def setMetadataValue(self, k, v):
    pickled = dumps(v, protocol=PICKLE_PROTOCOL)
    sql = "INSERT INTO db_metadata (key, value) VALUES (%s, %s) ON CONFLICT (key) DO "\
          "UPDATE SET value=excluded.value"
    csr = self.getCursor()
    csr.execute(sql, (k, psycopg2.Binary(pickled)))
    self.conn.commit()


  def getMetadataValue(self, k, default=None):
    csr = self.getCursor()
    sql = "SELECT value FROM db_metadata WHERE key=%s"
    try:
      csr.execute(sql, (k,))
      pickled = csr.fetchone()[0]
      self._L.debug("Pickled = %s", pickled)
      v = loads(pickled)
      return v
    except Exception as e:
      self._L.warning("Returning default value for %s", k)
    return default


  def deleteMetadataValue(self, k):
    self._L.info("Deleting metadata key %s", k)
    csr = self.getCursor()
    sql = "DELETE FROM db_metadata WHERE key=%s"
    csr.execute(sql, (k,))
    self.conn.commit()


  def getMetadata(self):
    res = collections.OrderedDict()
    csr = self.getCursor()
    sql = "SELECT key, value FROM db_metadata;"
    csr.execute(sql)
    for row in self._iterRow(csr, num_rows=10):
      k = row[0]
      pickled = row[1]
      v = loads(pickled)
      res[k] = v
    return res

