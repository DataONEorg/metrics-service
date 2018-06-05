import logging
import psycopg2
import configparser
import collections
import json
try:
  from cPickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
except ImportError:
  from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
from d1_metrics import common
from d1_metrics.metricselasticsearch import MetricsElasticSearch
import requests
from d1_metrics import solrclient


CONFIG_DATABASE_SECTION = "database"
DEFAULT_DB_CONFIG = {
  "host":"localhost",
  "port":"5432",
  "dbname":"metrics",
  "user":"metrics",
  "password":""
  }


class MetricsDatabase(object):
  '''
  Implements a wrapper and convenience methods for accessing the metrics postgresql database.
  '''

  def __init__(self, config_file=None):
    self._L = logging.getLogger(self.__class__.__name__)
    self.conn = None
    self._config = DEFAULT_DB_CONFIG
    if not config_file is None:
      self.loadConfig(config_file)


  def loadConfig(self, config_file):
    '''
    Load configuration parameters

    Args:
      config_file: Path to an INI format configuration file.

    Returns:
      dictionary of configuration values
    '''
    config = configparser.ConfigParser()
    self._L.debug("Loading configuration from %s", config_file)
    config.read(config_file)
    for key, value in iter(self._config.items()):
      self._config[key] = config.get(CONFIG_DATABASE_SECTION, key, fallback=value)
    return self._config


  def connect(self, force_new=False):
    '''
    Establish a connection to the postgres database

    Args:
      force_new: If true, force a new connection to the database.

    Returns:
      None
    '''
    if not self.conn is None and not force_new:
      self._L.info("Connection to database already established.")
      return
    self._L.info("Connecting to {user}@{host}:{port}/{dbname}".format(**self._config))
    self.conn = psycopg2.connect(**self._config)


  def getCursor(self):
    '''
    Retrieve a cursor for the postgres database, opening connection if necessary.

    Returns:
      cursor to the database
    '''
    self.connect()
    return self.conn.cursor()


  def _iterRow(self, cursor, num_rows=100):
    '''
    Iterator method for access to query results.

    Args:
      cursor: The cursor that executed the query
      num_rows: Number of rows to retrieve at a time.

    Returns:

    '''
    while True:
      rows = cursor.fetchmany(num_rows)
      if not rows:
        break
      for row in rows:
        yield row


  def getSingleValue(self, csr, sql):
    '''
    Retrieve a single value from the resultset identifed by a SQL statement

    Args:
      csr: Cursor to use
      sql: SQL statement to execute.

    Returns:
      First value of the first record responsive to query.
    '''
    self._L.debug("getSingleValue: %s", sql)
    csr.execute(sql)
    row = csr.fetchone()
    return row[0]


  def initializeDatabase(self, sql_files):
    '''
    Initialize the database by executing prepared SQL commands

    Args:
      sql_files: List of file paths to execute

    Returns: None
    '''
    with self.getCursor() as csr:
      for sql_file in sql_files:
        self._L.info("Loading: %s", sql_file)
        sql = open(sql_file, "r", encoding="utf8").read()
        self._L.debug("Executing: %s", sql)
        csr.execute(sql)
    self.conn.commit()


  def summaryReport(self):
    '''
    Gather basic stats about the database content.

    Returns: Dictionary giving count of rows in various views plus the metadata K,V pairs.

    '''
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
    '''
    Set a K,V pair in the metadata table

    Args:
      k: key for value
      v: pickle-able value to store

    Returns: None

    '''
    pickled = dumps(v, protocol=PICKLE_PROTOCOL)
    sql = "INSERT INTO db_metadata (key, value) VALUES (%s, %s) ON CONFLICT (key) DO "\
          "UPDATE SET value=excluded.value"
    csr = self.getCursor()
    csr.execute(sql, (k, psycopg2.Binary(pickled)))
    self.conn.commit()


  def getMetadataValue(self, k, default=None):
    '''
    Retrieve a value from the metadata table.

    Args:
      k: Key of value to retrieve
      default: value to return if key not available.

    Returns: un-pickled value from metadta table or default if no value.

    '''
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
    '''
    Remove a metadata entry

    Args:
      k: Key of entry to remove

    Returns: None

    '''
    self._L.info("Deleting metadata key %s", k)
    csr = self.getCursor()
    sql = "DELETE FROM db_metadata WHERE key=%s"
    csr.execute(sql, (k,))
    self.conn.commit()


  def getMetadata(self):
    '''
    Retrieve all the metadata entries
    Returns: ordered dictionary of Key,Value

    '''
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


  def getSummaryMetricsPerDataset(self):
    '''
    Method that queries the DB materialized views
    for the dataset landing page.
    :return: Dictionary object containing all the results
    '''
    res = dict()
    csr = self.getCursor()
    sql = "select * from landingpage3 where dataset_id in (\'" \
          + "\',\'".join(request) + "\') " \
          + "group by month, year, metrics_name, sum, dataset_id order by month, year;"
    csr.execute(sql)
    # retrieving the results
    rows = csr.fetchall()
    # appending the results to a list and
    # returning it to the MetricsHandler class
    for items in res:
      if items[1] in res:
        res[items[1]].append(str(items[4]))
      else:
        res[items[1]] = []
        res[items[1]].append(str(items[4]))

      if 'Months' in res:
        if str(items[2]) + "-" + str(items[3]) in res['Months']:
          pass
        else:
          res['Months'].append(str(items[2]) + "-" + str(items[3]))
      else:
        res['Months'] = []
        res['Months'].append(str(items[2]) + "-" + str(items[3]))
    return res


  def getCitations(self):
    '''
    Gets citations from the crossref end point
    :return:
    '''

    dois, pref = self.getDOIs()
    csr = self.getCursor()
    sql = "INSERT INTO CITATIONS(id, target_id, source_id, relation_type, source_id_Scheme, source_id_url, " +\
          "source_type_name, source_sub_type, source_sub_type_schema, link_publication_date) values ( DEFAULT,'"

    count = 0
    for i in pref:
      res = requests.get("https://api.eventdata.crossref.org/v1/events/scholix?source=crossref&obj-id.prefix="+i)
      dict = res.json()
      for val in dict["message"]["link-packages"]:
        if (val["Target"]["Identifier"]["ID"] in dois):
          try:
            count = count + 1
            values = []
            values.append(val["Target"]["Identifier"]["ID"])
            values.append(val["Source"]["Identifier"]["ID"])
            values.append(val["RelationshipType"]["Name"])
            values.append(val["Source"]["Identifier"]["IDScheme"])
            values.append(val["Source"]["Identifier"]["IDUrl"])
            values.append(val["Source"]["Type"]["Name"])
            values.append(val["Source"]["Type"]["SubType"])
            values.append(val["Source"]["Type"]["SubTypeSchema"])
            values.append(val["LinkPublicationDate"])

            csr.execute(sql + "','".join(values) + "');")
            print("A new citation record inserted!")

          except psycopg2.DatabaseError as e:
            print('Database error!\n{0}' , e)
          except psycopg2.OperationalError as e:
            print('Operational error!\n{0}', e)
          finally:
            self.conn.commit()
    print("Queries executed!")
    return


  def getDOIs(self):
    """
    Scans the solr end point for DOIs
    :return: Set objects containing dois and their prefixes
    """
    cd = solrclient.SolrClient('https://cn-ucsb-1.dataone.org/cn/v2/query', 'solr')
    data = cd.getFieldValues('id', q='id:*doi*')
    res = []
    prefixes = []
    for hit in data['id'][::2]:
      start_doi = hit.index("10.")
      res.append(hit[start_doi:])
      prefixes.append(hit[start_doi:start_doi+7])
    dois = set(res)
    pref = set(prefixes)
    return dois, pref



if __name__ == "__main__":
  md = MetricsDatabase()
  md.getCitations()