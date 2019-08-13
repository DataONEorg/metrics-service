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
    "host": "localhost",
    "port": "5432",
    "dbname": "metrics",
    "user": "metrics",
    "password": ""
}


class MetricsDatabase(object):
    '''
    Implements a wrapper and convenience methods for accessing the metrics postgresql database.
    '''

    def __init__(self, config_file=None):
        self._L = logging.getLogger(self.__class__.__name__)
        self.conn = None
        self.solr_query_url = "https://cn.dataone.org/cn/v2/query/solr/"
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
            "version": "SELECT version FROM db_version;",
            "rows": "SELECT count(*) FROM metrics;",
        }
        with self.getCursor() as csr:
            for key, value in iter(operations.items()):
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
        sql = "INSERT INTO db_metadata (key, value) VALUES (%s, %s) ON CONFLICT (key) DO " \
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

    def getCitations(self):
        '''
        Gets citations from the crossref end point
        :return: None. Saves the citations in the Citation table of the MDC database
        '''

        dois, pref = self.getDOIs()
        csr = self.getCursor()
        sql = "INSERT INTO CITATIONS(id, report, metadata, target_id, source_id, source_url, link_publication_date, origin, title, " + \
              "publisher, journal, volume, page, year_of_publishing) values ( DEFAULT,'"

        count = 0
        hist = {}
        for i in pref:
            print()
            print("Executing for", i)

            res = requests.get(
                "https://api.eventdata.crossref.org/v1/events/scholix?source=crossref&obj-id.prefix=" + i)
            results = res.json()
            citCount = 0
            for val in results["message"]["link-packages"]:
                for doi in dois:
                    if (val["Target"]["Identifier"]["ID"]).lower() in doi.lower():
                        target_pid = val["Target"]["Identifier"]["ID"]
                        source_pid = val["Source"]["Identifier"]["ID"]
                        count = count + 1

                        try:
                            url = val["Source"]["Identifier"]["IDUrl"]
                            headers = {'Accept': 'application/json'}

                            values = []
                            values.append(target_pid)
                            values.append(source_pid)
                            values.append(url)
                            values.append(val["LinkPublicationDate"][:10])
                            mdata = requests.get(url, headers=headers)
                            if (mdata.status_code == 404):
                                agency = requests.get("https://api.crossref.org/works/" + source_pid + "/agency/")
                                agency_body = agency.json()
                                if (agency_body["message"]["agency"]["label"] == "DataCite"):
                                    mdata = requests.get("https://api.datacite.org/works/" + source_pid)
                                    metadata = mdata.json()
                                    author = []
                                    for i in metadata["data"]["attributes"]["author"]:
                                        if "given" in i:
                                            author.append((i["given"] + " " + i["family"]))
                                        else:
                                            author.append('')
                                    values.append(", ".join(author).replace("'", r"''"))
                                    values.append((metadata["data"]["attributes"]["title"]).replace("'", r"''"))
                                    values.append(
                                        (metadata["data"]["attributes"]["container-title"]).replace("'", r"''"))
                                    values.append(str(metadata["data"]["attributes"]["published"]))
                                if (agency_body["message"]["agency"]["label"] == "Crossref"):
                                    mdata = requests.get("https://api.crossref.org/works/" + source_pid)
                                    metadata = mdata.json()
                                    values.append((", ".join((i["given"] + " " + i["family"]) for i in
                                                             metadata["message"]["author"])).replace("'", r"''"))
                                    values.append((metadata["message"]["title"][0]).replace("'", r"''"))
                                    values.append((metadata["message"]["publisher"]).replace("'", r"''"))
                                    values.append(str(metadata["message"]["created"]["date-parts"][0][0]))
                            else:

                                try:
                                    metadata = mdata.json()
                                except:
                                    print(doi)
                                    continue

                                try:
                                    author = []
                                    for i in metadata["author"]:
                                        if "given" in i:
                                            author.append((i["given"] + " " + i["family"]))
                                        elif "name" in i:
                                            author.append(i["name"])
                                        else:
                                            author.append('')
                                    values.append(", ".join(author).replace("'", r"''"))
                                    values.append(metadata["title"].replace("'", r"''"))
                                    values.append(metadata["publisher"].replace("'", r"''"))
                                    if "container-title" in metadata:
                                        values.append(metadata["container-title"].replace("'", r"''"))
                                    else:
                                        values.append('NULL')
                                    if "volume" in metadata:
                                        values.append(metadata["volume"].replace("'", r"''"))
                                    else:
                                        values.append('NULL')
                                    if "page" in metadata:
                                        values.append(metadata["page"].replace("'", r"''"))
                                    else:
                                        values.append('NULL')
                                    values.append(str(metadata["created"]["date-parts"][0][0]))
                                    csr.execute(sql + (json.dumps(results)).replace("'", r"''") + "','" + (
                                    json.dumps(metadata)).replace("'", r"''") + "','" + "','".join(values) + "');")
                                    if count%5 == 0:
                                        print("Citation Insertion Count ", str(count))
                                except:
                                    print("Exception occured")
                                    print("DOI Error: {0:0=3d}".format(count), " - ", doi)

                        except psycopg2.DatabaseError as e:
                            print('Database error!\n{0}', e)
                            print()
                        except psycopg2.OperationalError as e:
                            print('Operational error!\n{0}', e)
                            print()
                        finally:
                            self.conn.commit()
                            pass
                        break

        print("Queries executed!")
        return

    def updateCitationMetadata(self):
        """
        This function queries the Crossref end poi for metadata of the sources that cited our datasets.
        This is meant to keep the metadata up to ddate in our system.
        :return:
        """
        csr = self.getCursor()

        getSourcePIDs = "SELECT source_id FROM CITATIONS;"
        sql = "UPDATE CITATIONS SET (origin, title, publisher, year_of_publishing) = ('"
        pref = []
        try:
            csr.execute(getSourcePIDs)
            results = csr.fetchall()
            for i in results:
                start_doi = i[0].index("10.")
                pref.append(i[0][start_doi:])
        except psycopg2.DatabaseError as e:
            print('Database error!\n{0}', e)
        except psycopg2.OperationalError as e:
            print('Operational error!\n{0}', e)
        finally:
            self.conn.commit()

        for doi in pref:
            try:
                values = []
                url = 'https://doi.org/' + doi
                headers = {'Accept': 'application/x-bibtex'}
                mdata = requests.get(url, headers=headers)
                if (mdata.status_code == 404):
                    agency = requests.get("https://api.crossref.org/works/" + doi + "/agency/")
                    agency_body = agency.json()
                    if (agency_body["message"]["agency"]["label"] == "DataCite"):
                        print("DataCite")
                        mdata = requests.get("https://api.datacite.org/works/" + doi)
                        metadata = mdata.json()
                        author = []
                        for i in metadata["data"]["attributes"]["author"]:
                            if "given" in i:
                                author.append((i["given"] + " " + i["family"]))
                            else:
                                author.append(i["literal"])
                        values.append(", ".join(author).replace("'", r"''"))
                        values.append((metadata["data"]["attributes"]["title"]).replace("'", r"''"))
                        values.append((metadata["data"]["attributes"]["container-title"]).replace("'", r"''"))
                        values.append(str(metadata["data"]["attributes"]["published"]))
                    if (agency_body["message"]["agency"]["label"] == "Crossref"):
                        print("Crossref")
                        mdata = requests.get("https://api.datacite.org/works/" + doi)
                        metadata = mdata.json()
                        values.append(
                            (
                            ", ".join((i["given"] + " " + i["family"]) for i in metadata["message"]["author"])).replace(
                                "'", r"''"))
                        values.append((metadata["message"]["title"][0]).replace("'", r"''"))
                        values.append((metadata["message"]["publisher"]).replace("'", r"''"))
                        values.append(str(metadata["message"]["created"]["date-parts"][0][0]))
                else:
                    # Format the response retrieved from the doi resolving endpoint and save it to a dictionary
                    print("DOI Endpoint")
                    mdata_resp = mdata.text[6:-1]
                    mdata_list = mdata_resp.split("\n")
                    metadata = {}
                    for i in mdata_list:
                        key = i.split("=")
                        if len(key) > 1:
                            key[0] = key[0].strip()
                            metadata[key[0]] = key[1][key[1].find("{") + 1:key[1].rfind("}")]
                    values.append(metadata["author"].replace("'", r"''"))
                    values.append(metadata["title"].replace("'", r"''"))
                    values.append(metadata["publisher"].replace("'", r"''"))
                    values.append(metadata["year"].replace("'", r"''"))
                condition = "WHERE source_id = '" + doi + "'"
                csr.execute(sql + "', '".join(values) + "')" + condition + ";")
                print("Record updated")

            except psycopg2.DatabaseError as e:
                print('Database error!\n{0}', e)
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
        cd = solrclient.SolrClient('https://cn.dataone.org/cn/v2/query', 'solr')
        data = cd.getFieldValues('id', q='id:/.*doi.*{1,5}10\.[0-9]{4,6}.*/')
        seriesIdData = cd.getFieldValues('seriesId', q='seriesId:/.*doi.*{1,5}10\.[0-9]{4,6}.*/')
        prefixes = []
        doi = []
        count = 0

        # Performing parsing of the identifiers that have DOI
        for hit in data['id'][::2]:
            count += 1

            try:
                start_doi = hit.index("10.")
            except:
                print(hit)
                continue

            doi.append(hit)
            prefixes.append(hit[start_doi:start_doi + 7])

        count = 0

        # Performing parsing of the series identifiers that have DOI
        for hit in seriesIdData['seriesId'][::2]:
            count += 1

            try:
                start_doi = hit.index("10.")
            except:
                print(hit)
                continue

            doi.append(hit)
            prefixes.append(hit[start_doi:start_doi + 7])
        dois = set(doi)
        pref = set(prefixes)
        print(pref)
        return dois, pref

    def getTargetCitationMetadata(self):
        """
        This method gets the target citation metadata which are basically the facets like authors,
        nodeId, awards and funding information
        This information will mostly be used for easy retrieval of citation for various profile pages
        :return:
        """
        # TODO Think of other possible facets that you'll require in the future

        citation_pids = "SELECT DISTINCT target_id FROM CITATIONS;"
        citation_metadata_pids = "SELECT DISTINCT target_id FROM CITATION_METADATA;"
        csr = self.getCursor()
        try:
            csr.execute(citation_pids)
            target_pids = []
            for i in csr.fetchall():
                target_pids.append(i[0])
            csr.execute(citation_metadata_pids)
            saved_pids = csr.fetchall()

            for i in saved_pids:
                if i[0] in target_pids:
                    target_pids.remove(i[0])

            unique_pids = set(target_pids)

            for i in unique_pids:
                response = self.query_solr(q="*" + i + "*")
                results = response["response"]
                if results["numFound"] > 0:
                    for j in results["docs"]:
                        if "origin" in j and "authoritativeMN" in j:
                            origin = j["origin"]
                            authoritativeMN = j["authoritativeMN"]

                            for k in range(len(origin)):
                                if "," in origin[k]:
                                    origin[k] = origin[k].replace(",", r"\,")

                            csr.execute("INSERT INTO CITATION_METADATA VALUES (DEFAULT,'"+i.replace("'", r"''")+"','{" + (",".join(origin)).replace("'", r"''")+"}','{" + authoritativeMN.replace("'", r"''") +"}',NULL,NULL);")
                            break

        except psycopg2.DatabaseError as e:
            print('Database error!\n{0}', e)
            print()
        except psycopg2.OperationalError as e:
            print('Operational error!\n{0}', e)
            print()
        finally:
            self.conn.commit()
            pass

    def query_solr(self, q):
        """
        Queries the Solr end-point for metadata given the PID.
        :param q: Query param
        :return: JSON Object containing the metadata fields queried from Solr
        """

        queryString = 'q=id:*' + q + '* OR id:*' + q.lower() + '* OR id:*' + q.upper() + 
                      '* OR seriesId:*' + q + '* OR seriesId:*' + q.lower() + 
                      '* OR seriesId:*' + q.upper() + '*&fl=origin,authoritativeMN&wt=json'

        response = requests.get(url=self.solr_query_url, params=queryString)

        return response.json()


if __name__ == "__main__":
    md = MetricsDatabase()
    md.getTargetCitationMetadata()
    # md.getDOIs()