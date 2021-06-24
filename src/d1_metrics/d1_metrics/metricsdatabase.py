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
from datetime import datetime, timedelta

CONFIG_DATABASE_SECTION = "database"
DEFAULT_DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "dbname": "metrics",
    "user": "metrics",
    "password": ""
}
SOLR_RESERVED_CHAR_LIST = [
  '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*',
  '?', ':'
]
SOLR_QUERY_URL = "https://cn-secondary.dataone.org/cn/v2/query/solr/"
CN_URL = "https://cn-secondary.dataone.org/cn/v2/query"


class MetricsDatabase(object):
    '''
    Implements a wrapper and convenience methods for accessing the metrics postgresql database.
    '''

    def __init__(self, config_file=None):
        self._L = logging.getLogger(self.__class__.__name__)
        self.conn = None
        self.solr_query_url = SOLR_QUERY_URL
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


    def logConfig(self, filename=None, format=None, level=None):
        """
        Sets log config if specified

        :param filename: logger file name
        :param format: format for the logging message
        :param level: level of the logger

        :return:
            None
        """

        if filename is not None:
            hdlr = logging.FileHandler(filename)
            if format is not None:
                formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
                hdlr.setFormatter(formatter)
            self._L.addHandler(hdlr)

        if level is None:
            self._L.setLevel(logging.WARNING)
        elif level == "INFO":
            self._L.setLevel(logging.INFO)
        elif level == "DEBUG":
            self._L.setLevel(logging.DEBUG)
        elif level == "ERROR":
            self._L.setLevel(logging.ERROR)
        elif level == "CRITICAL":
            self._L.setLevel(logging.CRITICAL)

        return


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
        sql = "INSERT INTO CITATIONS (id, report, metadata, target_id, source_id, source_url, link_publication_date, origin, title, " + \
              "publisher, journal, volume, page, year_of_publishing) values ( DEFAULT,'"

        count = 0
        hist = {}
        for i in pref:
            self._L.info()
            self._L.info("Executing for" + i)

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
                            if (mdata.status_code != response.codes.ok):
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
                                except Exception as e:
                                    self._L.exception("Metadata conversion error")
                                    self._L.exception(e)
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
                                        self._L.info("Citation Insertion Count " + str(count))
                                except:
                                    self._L.exception("Exception occured")
                                    self._L.exception("DOI Error: {0:0=3d}".format(count) + " - " + doi)

                        except psycopg2.DatabaseError as e:
                            self._L.exception('Database error!\n{0}')
                            self._L.exception(e)
                        except psycopg2.OperationalError as e:
                            self._L.exception('Operational error!\n{0}')
                            self._L.exception(e)
                        finally:
                            self.conn.commit()
                            pass
                        break

        self._L.info("Queries executed!")
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
            self._L.exception('Database error!\n{0}')
            self._L.exception(e)
        except psycopg2.OperationalError as e:
            self._L.exception('Operational error!\n{0}')
            self._L.exception(e)
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

                        mdata = requests.get("https://api.crossref.org/works/" + doi)
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

                    mdata_resp = mdata.text[6:-1]
                    mdata_list = mdata_resp.split("\n")
                    metadata = {}
                    for i in mdata_list:
                        key = i.split("=")
                        if len(key) > 1:
                            key[0] = key[0].strip()
                            metadata[key[0]] = key[1][key[1].find("{") + 1:key[1].rfind("}")]
                    if "author" in metadata:
                        values.append(metadata["author"].replace("'", r"''"))
                    else:
                        values.append(None)
                    if "title" in metadata:
                        values.append(metadata["title"].replace("'", r"''"))
                    else:
                        values.append(None)
                    if "publisher" in metadata:
                        values.append(metadata["publisher"].replace("'", r"''"))
                    else:
                        values.append(None)
                    if "year" in metadata:
                        values.append(metadata["year"].replace("'", r"''"))
                    else:
                        values.append(None)
                condition = "WHERE source_id = '" + doi + "'"
                csr.execute(sql + "', '".join(values) + "')" + condition + ";")
                self._L.info("Record updated")

            except psycopg2.DatabaseError as e:
                self._L.exception('Database error!\n{0}')
                self._L.exception(e)
            except psycopg2.OperationalError as e:
                self._L.exception('Operational error!\n{0}')
                self._L.exception(e)
            finally:
                self.conn.commit()

        self._L.info("Queries executed!")
        return


    def getDOIs(self, cn_url=None):
        """
        Scans the solr end point for DOIs
        :return: Set objects containing dois and their prefixes
        """
        if cn_url is None:
            cn_url = CN_URL
        cd = solrclient.SolrClient(cn_url, 'solr')
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
                self._L.info(hit)
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
                self._L.info(hit)
                continue

            doi.append(hit)
            prefixes.append(hit[start_doi:start_doi + 7])
        dois = set(doi)
        pref = set(prefixes)
        self._L.info(pref)
        return dois, pref


    def getTargetCitationMetadata(self, operation="UPDATE"):
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

            if operation != "UPDATE":
                csr.execute(citation_metadata_pids)
                saved_pids = csr.fetchall()

                for i in saved_pids:
                    if i[0] in target_pids:
                        target_pids.remove(i[0])

            unique_pids = set(target_pids)

            for i in unique_pids:
                # check if https DOI format
                if "https" in i:
                    self._L.info("DOI https format: " + i)
                    pass

                # if the doi identifier is missing DOI keyword;
                if i[:3] == "10.":
                    identifier = "doi:" + i
                    self._L.info("DOI format : " + identifier)

                response = self.query_solr(q=self.escapeSolrQueryTerm(i))

                if len(response) > 0:
                    results = response["response"]
                    if results["numFound"] > 0:
                        for j in results["docs"]:
                            if "origin" in j and "authoritativeMN" in j:
                                origin = j["origin"]
                                authoritativeMN = j["authoritativeMN"]

                                for k in range(len(origin)):
                                    if "," in origin[k]:
                                        origin[k] = origin[k].replace(",", r"\,")
                                    if '"' in origin[k]:
                                        origin[k] = origin[k].replace('"', r'\"')

                            title = ""
                            if "title" in j:
                                title = j["title"]

                            dateUploaded = ""
                            if "dateUploaded" in j:
                                dateUploaded = j["dateUploaded"]

                            dateModified = ""
                            if "dateModified" in j:
                                dateModified = j["dateModified"]

                            datePublished = ""
                            if "datePublished" in j:
                                datePublished = j["datePublished"]

                            if operation == "UPDATE":
                                csr_statement = "UPDATE CITATION_METADATA SET " +\
                                            " title = '" + title.replace("'", r"''") + "', datePublished = '" + datePublished.replace("'", r"''") + "', dateUploaded = '" + dateUploaded.replace("'", r"''") + "', dateModified = '" + dateModified.replace("'", r"''") + "' " +\
                                            "WHERE target_id='" + i + "';"
                                print(csr_statement)
                            else:

                                csr_statement = "INSERT INTO CITATION_METADATA (id, target_id, origin, node_id, title, datepublished, dateuploaded, datemodified) VALUES (DEFAULT,'" + \
                                                i.replace("'", r"''")+"','{" + (",".join(origin)).replace("'", r"''")+"}','{" + \
                                                authoritativeMN.replace("'", r"''") +"}','" + title.replace("'", r"''") + "','" + datePublished.replace("'", r"''") + "','" + dateUploaded.replace("'", r"''") + "','" + dateModified.replace("'", r"''") + "');"
                            csr.execute(csr_statement)
                            break
                else:
                    self._L.exception("solr error for id: " + i)
                    self._L.exception(response)

        except psycopg2.DatabaseError as e:
            self._L.exception('Database error!\n{0}')
            self._L.exception(e)
        except psycopg2.OperationalError as e:
            self._L.exception('Operational error!\n{0}')
            self._L.exception(e)
        finally:
            self.conn.commit()
        return


    def updateTargetCitationMetadata(self):
        """
        This method gets the target citation metadata which are basically the facets like authors,
        nodeId, awards and funding information
        This information will mostly be used for easy retrieval of citation for various profile pages
        :return:
        """
        # TODO Think of other possible facets that you'll require in the future

        citation_metadata_pids = "SELECT DISTINCT target_id FROM CITATION_METADATA WHERE id >= 1330;"
        csr = self.getCursor()
        try:
            csr.execute(citation_metadata_pids)
            saved_pids = csr.fetchall()
            target_pids = []

            for i in saved_pids:
                target_pids.append(i[0])

            unique_pids = set(target_pids)

            for i in unique_pids:

                # check if https DOI format
                if "https" in i:
                    self._L.info("DOI https format: " + i)
                    pass

                # if the doi identifier is missing DOI keyword;
                if i[:3] == "10.":
                    identifier = "doi:" + i
                    self._L.info("DOI format : " + identifier)

                response = self.query_solr(q=self.escapeSolrQueryTerm(i))

                if len(response) > 0:
                    results = response["response"]
                    if results["numFound"] > 0:
                        for j in results["docs"]:
                            if "title" in j:
                                title = j["title"]
                            else:
                                title = ""

                            if "dateUploaded" in j:
                                dateUploaded = j["dateUploaded"]
                            else:
                                dateUploaded = ""

                            if "datePublished" in j:
                                datePublished = j["datePublished"]
                            else:
                                datePublished = ""

                            if "dateModified" in j:
                                dateModified = j["dateModified"]
                            else:
                                dateModified = ""

                            stmt = ("UPDATE CITATION_METADATA SET title='" + title.replace("'", r"''") + "', datePublished='" + datePublished.replace("'", r"''") + "', dateUploaded='" + dateUploaded.replace("'", r"''") + "', dateModified='" + dateModified.replace("'", r"''") + "' WHERE target_id='" + i + "';")
                            # print(stmt)
                            csr.execute(stmt)
                            break
                else:
                    self._L.exception("solr error for id: " + i)
                    self._L.exception(response)

        except psycopg2.DatabaseError as e:
            self._L.exception('Database error!\n{0}')
            self._L.exception(e)
        except psycopg2.OperationalError as e:
            self._L.exception('Operational error!\n{0}')
            self._L.exception(e)
        finally:
            self.conn.commit()
        return


    def query_solr(self, an_id):
        """
        Queries the Solr end-point for metadata given the PID.
        :param q: Query param
        :return: JSON Object containing the metadata fields queried from Solr
        """

        queryString = "q=(((id:*" + an_id + "*) OR (id:*" + an_id.upper() + "*) OR (id:*" + an_id.lower() + \
                        "*) OR (seriesId:*" + an_id + "*) OR (seriesId:*" + an_id.upper() + \
                        "*) OR (seriesId:*" + an_id.lower() + "*) OR (resourceMap:*" + an_id + \
                        "*) OR (resourceMap:*" + an_id.upper() + "*) OR (resourceMap:*" + an_id.lower() + \
                        "*)) AND formatType:METADATA) &fl=id,seriesId,origin,authoritativeMN,title,dateUploaded,datePublished,dateModified&wt=json&rows=1"

        response = requests.get(url=self.solr_query_url, params=queryString)

        if response.status_code == 200:
            return response.json()

        return {}


    def getDOIMetadata(self, doi):
        """
        Takes a DOI identifier string and fetches it metadata

        :param doi:
        :return:
        """
        doi_metadata = {}
        doi.replace(" ", "")

        try:
            # get the DOI resolving agency
            agency = requests.get("https://api.crossref.org/works/" + doi + "/agency/")
            agency_body = agency.json()
        except Exception as e:
            agency_body = {}
            agency_body["message"] = {}
            agency_body["message"]["agency"] = {}
            agency_body["message"]["agency"]["label"] = "Crossref"

        try:

            # If Datacite DOI - query Datacite REST endpoint
            if (agency_body["message"]["agency"]["label"] == "DataCite"):
                doi_metadata["link_publication_date"] = datetime.today().strftime('%Y-%m-%d')
                mdata = requests.get("https://api.datacite.org/works/" + doi)
                metadata = mdata.json()
                author = []
                for i in metadata["data"]["attributes"]["author"]:
                    if "given" in i:
                        author.append((i["given"] + " " + i["family"]))
                    else:
                        author.append(i["literal"])
                doi_metadata["origin"] = author
                doi_metadata["title"] = metadata["data"]["attributes"]["title"]
                doi_metadata["publisher"] = metadata["data"]["attributes"]["container-title"]
                doi_metadata["year_of_publishing"] = str(metadata["data"]["attributes"]["published"])
                doi_metadata["source_url"] = "https://doi.org/" + doi

            # If Crossref DOI - query Crossref REST endpoint
            if (agency_body["message"]["agency"]["label"] == "Crossref"):

                doi_metadata["link_publication_date"] = datetime.today().strftime('%Y-%m-%d')
                mdata = requests.get("https://api.crossref.org/works/" + doi)
                metadata = mdata.json()
                author = []
                for i in metadata["message"]["author"]:
                    if "given" in i:
                        author.append((i["given"] + " " + i["family"]))
                doi_metadata["origin"] =  author

                if "title" in metadata["message"]:
                    doi_metadata["title"] = metadata["message"]["title"][0]
                else:
                    doi_metadata["title"] = 'NULL'

                if "publisher" in metadata["message"]:
                    doi_metadata["publisher"] = metadata["message"]["publisher"]
                else:
                    doi_metadata["publisher"] = 'NULL'

                doi_metadata["year_of_publishing"] = str(metadata["message"]["created"]["date-parts"][0][0])

                doi_metadata["source_url"] = "https://doi.org/" + doi

                if "container-title" in metadata["message"] and len(metadata["message"]["container-title"]) > 0:
                    doi_metadata["journal"] = metadata["message"]["container-title"][0]
                else:
                    doi_metadata["journal"] = 'NULL'

                if "volume" in metadata["message"]:
                    doi_metadata["volume"] = metadata["message"]["volume"]
                else:
                    doi_metadata["volume"] = 'NULL'

                if "page" in metadata["message"]:
                    doi_metadata["page"] = metadata["message"]["page"]
                else:
                    doi_metadata["page"] = 'NULL'

        except Exception as e:
            self._L.exception('DOI Metadata Resolution error!' + doi + "\n")
            self._L.exception(e)

        return doi_metadata


    def insertCitationObjects(self, citations_data = None, read_citations_from_file = None):
        """
        This method performs the insertion of citation objects to the DataONE Citations database
        The citations getting iserted could be either one of the following:
            1. A citation retrieved from the Crossref / DataONE citation
            2. A citation read from the JSON file containing citation object
            3. A citation request registered to the Citations endpoint

        :param citations_data:
        :param read_citations_from_file: the name of the file containing metadata
        :return:
        """

        # Assign a default empty value if citations data is not provided
        if not citations_data:
            citations_data = []

        # Read the metadata from the file object if file is provided
        if read_citations_from_file:
            citations_data = self.parseCitationsFromDisk(read_citations_from_file)

        csr = self.getCursor()
        sql = "INSERT INTO CITATIONS (id, report, metadata, target_id, source_id, source_url, link_publication_date, origin, title, " + \
              "publisher, journal, volume, page, year_of_publishing, reporter, relation_type) values ( DEFAULT,'"

        for citation_object in citations_data:
            self._L.info("\n")
            self._L.info("Executing for " + json.dumps(citation_object))

            results = {}
            metadata = citation_object
            values = []

            try:
                if "target_id" and "source_id" not in citation_object:
                    self._L.error("Source Id or Target Id missing")
                    self._L.error(json.dumps(citation_object))
                    continue
                try:
                    values.append(citation_object["target_id"].replace("'", r"''"))
                    values.append(citation_object["source_id"].replace("'", r"''"))
                    if "source_url" in citation_object:
                        values.append(citation_object["source_url"].replace("'", r"''"))
                    else:
                        values.append('NULL')
                    values.append(citation_object["link_publication_date"])
                    values.append(", ".join(citation_object["origin"]).replace("'", r"''"))
                    if "title" in citation_object:
                        values.append(citation_object["title"].replace("'", r"''"))
                    else:
                        values.append('NULL')
                    if "publisher" in citation_object:
                        values.append(citation_object["publisher"].replace("'", r"''"))
                    else:
                        citation_object
                    if "journal" in citation_object:
                        values.append(citation_object["journal"].replace("'", r"''"))
                    else:
                        values.append('NULL')
                    if "volume" in citation_object:
                        values.append(citation_object["volume"].replace("'", r"''"))
                    else:
                        values.append('NULL')
                    if "page" in citation_object:
                        values.append(citation_object["page"].replace("'", r"''"))
                    else:
                        values.append('NULL')
                    values.append(str(citation_object["year_of_publishing"]))
                    if "reporter" in citation_object:
                        values.append(citation_object["reporter"].replace("'", r"''"))
                    else:
                        values.append('NULL')
                    if "relation_type" in citation_object:
                        values.append(citation_object["relation_type"].replace("'", r"''"))
                    else:
                        values.append('NULL')
                except Exception as e:
                    self._L.exception('Exception occured!\n{0}')
                    self._L.exception(e)
                    self._L.exception("Object missing information - " + json.dumps(citation_object))
                    print("Object missing information")
                    print(source_id, target_id, e)
                    continue

                csr.execute(sql + (json.dumps(results)).replace("'", r"''") + "','" + (
                    json.dumps(metadata)).replace("'", r"''") + "','" + "','".join(values) + "');")

                self._L.info(
                    "SUCCESSFULLY INSERTED " + citation_object["target_id"] + " - " + citation_object[
                        "source_id"])

                print("success")

            except psycopg2.DatabaseError as e:
                self._L.exception('Operational error!\n{0}')
                self._L.exception(e)
                print("except")
            except psycopg2.OperationalError as e:
                self._L.exception('Operational error!\n{0}')
                self._L.exception(e)
                print("except")
            except Exception as e:
                self._L.exception('Exception occured!\n{0}')
                self._L.exception(e)
                print("except - " +  str(e))

            finally:
                self.conn.commit()

        return


    def parseCitationsFromDisk(self, file_name):
        """
        Reads the citation objects from file

        :param file_name:

        :return: an array of citaiton objects to be inserted into the Metrics Service
        """
        file_object = open(file_name,)

        # reading in the JOSN object
        citation_data = json.load(file_object)

        return citation_data


    def queueCitationRequest(self, request_object):
        """
        Stores the Citation request object to the database
        :param request_object:
        :return: None
        """
        csr = self.getCursor()

        # citation_source - by default is the API
        citation_source = "DataONE Metrics Service"
        if "citation_source" in request_object:
            citation_source = request_object["citation_source"]

        sql = "INSERT INTO citations_registration_queue (id, request, citation_source, receive_timestamp, ingest_attempts) VALUES ( DEFAULT, '"
        try:
            csr.execute(sql + (json.dumps(request_object)).replace("'", r"''") + "','" + citation_source.replace("'", r"''") + "','"  + str(datetime.now()) + "',0);" )

        except psycopg2.DatabaseError as e:
            self._L.exception('Database error!\n{0}')
            self._L.exception(e)
        except psycopg2.OperationalError as e:
            self._L.exception('Operational error!\n{0}')
            self._L.exception(e)
        except Exception as e:
            self._L.exception('Exception occured!\n{0}')
            self._L.exception(e)
        finally:
            self.conn.commit()
        return


    def parseQueuedCitationRequests(self):
        """
        Retrieves parsed citation from citations registration queue
        :return:
        """
        csr = self.getCursor()
        li_citation_objects = []

        sql = "SELECT * FROM citations_registration_queue where id > 77;"
        try:
            csr.execute(sql)
            li_citation_objects = csr.fetchall()

        except psycopg2.DatabaseError as e:
            self._L.exception('Database error!\n{0}')
            self._L.exception(e)
        except psycopg2.OperationalError as e:
            self._L.exception('Operational error!\n{0}')
            self._L.exception(e)
        finally:
            self.conn.commit()

        print("lenght : ", len(li_citation_objects))

        return li_citation_objects


    def processCitationQueueObject(self, citations_request):
        """
        Handles registration of a Citations queue object to the Citations table
        :param citations_request:
        :return:
        """

        try:
            if "citations" in citations_request and citations_request["citations"][0]["related_identifiers"][0]["identifier"] is not None:
                target_id = citations_request["citations"][0]["related_identifiers"][0]["identifier"]
                if target_id.find("10.") > -1:
                    target_doi_start = target_id.index("10.")
                    target_id = target_id[target_doi_start:]
            elif "metadata" in citations_request and citations_request["metadata"][0]["target_id"] is not None:
                target_id = citations_request["metadata"][0]["target_id"]
                if target_id.find("10.") > -1:
                    target_doi_start = target_id.index("10.")
                    target_id = target_id[target_doi_start:]
            else:
                self._L.error("No target_id found")

            try:
                if "citations" in citations_request and citations_request["citations"][0]["source_id"] is not None:
                    source_id = citations_request["citations"][0]["source_id"]
                    try:
                        source_doi_start = source_id.index("10.")
                        source_doi = source_id[source_doi_start:]
                    except:
                        self._L.error("Source is not a DOI")
                        return
                elif "metadata" in citations_request and citations_request["metadata"][0]["source_id"] is not None:
                    source_id = citations_request["metadata"][0]["source_id"]
                    try:
                        source_doi_start = source_id.index("10.")
                        source_doi = source_id[source_doi_start:]
                    except:
                        self._L.error("Source is not a DOI")
                        return
                else:
                    self._L.error("No source_id found")
            except ValueError as e:
                self._L.error("Source ID has issues : " + str(e))
                self._L.info(source_id)
                self._L.info("Quitting the ingest process")
                return

            if "citations" in citations_request and citations_request["citations"][0]["related_identifiers"][0]["relation_type"] is not None:
                relation_type = citations_request["citations"][0]["related_identifiers"][0]["relation_type"]
            elif "relation_type" in citations_request and citations_request["relation_type"] is not None:
                relation_type = citations_request["relation_type"]
            else:
                relation_type = None
                self._L.error("No relation_type found")

            if "submitter" in citations_request and citations_request["submitter"] is not None:
                submitter = citations_request["submitter"]
            else:
                submitter = None
                self._L.error("No submitter found")

            # retrieve metadata from Metrics Database
            self._L.info(source_doi)
            citation_object = self.getDOIMetadata(source_doi)
            citation_object["source_id"] = source_doi.replace(" ", "")
            citation_object["target_id"] = target_id.replace(" ", "")
            citation_object["relation_type"] = relation_type
            # database schema has attribute reporter
            citation_object["reporter"] = submitter

            citations_data = []
            citations_data.append(citation_object)
            self.insertCitationObjects(citations_data=citations_data)
        except Exception as e:
            self._L.exception("Citation registration error " + target_id)
            self._L.exception(e)


    def parseCitationsRequestsFromDisk(self, file_name = None):
        file_object = open(file_name, )

        # reading in the JOSN object
        citation_data = json.load(file_object)

        citations_request = {
            "request_type": "dataset",
            "metadata":
                [
                    {
                        "target_id": "",
                        "source_id": "",
                        "relation_type": ""
                    }
                ]
        }

        citations_dict = self.getExistingCitationsData()

        for citations_raw_request in citation_data:
            if "relation_type" not in citations_raw_request:
                citations_raw_request["relation_type"] = "isReferencedBy"

            if "target_id" not in citations_raw_request:
                self._L.info("ERROR: no target id found")
                self._L.info(json.dumps(citations_raw_request))
                continue

            if "source_id" not in citations_raw_request:
                self._L.info("ERROR: no source id found")
                self._L.info(json.dumps(citations_raw_request))
                continue

            citations_request["metadata"][0]["target_id"] = citations_raw_request["target_id"]
            citations_request["metadata"][0]["source_id"] = citations_raw_request["source_id"]
            citations_request["metadata"][0]["relation_type"] = citations_raw_request["relation_type"]
            # print(json.dumps(citations_request))


            if not self.validateUniqueCitation(citations_raw_request["target_id"], citations_raw_request["source_id"], citations_dict):
                self._L.info("SUCCESSFULLY INSERTED " + citations_raw_request["target_id"] + " - " +citations_raw_request["source_id"])
                continue

            self.processCitationQueueObject(citations_request)
        return


    def registerQueuedCitationRequest(self):
        """
        Registers queued citation to Citations DB
        :return:
        """
        registered_list = self.parseQueuedCitationRequests()
        csr = self.getCursor()

        citations_dict = self.getExistingCitationsData()

        for citation_object in registered_list:
            print("trying : ", json.dumps(citation_object[1]))
            citation_object_id = citation_object[0]
            citation_object_request = citation_object[1]
            ingest_attempt = citation_object[4]
            try:
                if "metadata" in citation_object_request:
                    citation_object_request_metadata = citation_object_request["metadata"]
                    citation_object_request_target_id = citation_object_request_metadata[0]["target_id"]
                    citation_object_request_source_id = citation_object_request_metadata[0]["source_id"]
                    citation_object_request_relation_type = citation_object_request_metadata[0]["relation_type"]
                elif "citations" in citation_object_request:
                    citation_object_request_metadata = citation_object_request["citations"]
                    citation_object_request_target_id = citation_object_request_metadata[0]["related_identifiers"][0][
                        "identifier"]
                    citation_object_request_source_id = citation_object_request_metadata[0]["source_id"]
                    citation_object_request_relation_type = \
                    citation_object_request_metadata[0]["related_identifiers"][0]["relation_type"]
                else:
                    self._L.error("Metadata not provided")
                    continue

                if "request_type" in citation_object_request:
                    citation_object_request_type = citation_object_request["request_type"]
                else:
                    self._L.error("request_type not provided")
                    continue


                print("pass 1")

                if (citation_object_request_type == "dataset"):

                    if len(citation_object_request_target_id) > 0 and citation_object_request_target_id.find("10.") > -1:
                        target_doi_start = citation_object_request_target_id.index("10.")
                        citation_object_request_target_doi = citation_object_request_target_id[target_doi_start:]
                    else:
                        citation_object_request_target_doi = citation_object_request_target_id

                    if len(citation_object_request_source_id) > 0 and citation_object_request_source_id.find("10.") > -1:
                        source_doi_start = citation_object_request_source_id.index("10.")
                        citation_object_request_source_doi = citation_object_request_source_id[source_doi_start:]
                    else:
                        citation_object_request_source_doi = citation_object_request_source_id

                    print("pass 2")
                    if not self.validateUniqueCitation(citation_object_request_target_id, citation_object_request_source_id, citations_dict):
                        print("pass 3")
                        self._L.info(
                            "SUCCESSFULLY INSERTED " + citation_object_request_target_id + " - " + citation_object_request_source_id)
                        continue

                    print("pass 4")

                    try:
                        print("beginning " + citation_object_request_target_id)
                        self.processCitationQueueObject(citation_object_request)
                        ingest_timestamp = datetime.now()
                        print("pass 5")

                        sql = "UPDATE citations_registration_queue SET ingest_timestamp = '" + str(
                            datetime.now()) + "' WHERE id = " + str(citation_object_id) + ";"
                        try:
                            csr.execute(sql)
                            print("pass 6")

                        except psycopg2.DatabaseError as e:
                            self._L.exception('Operational error!\n{0}')
                            self._L.exception(e)

                        except psycopg2.OperationalError as e:
                            self._L.exception('Operational error!\n{0}')
                            self._L.exception(e)

                    except Exception as e:
                        self._L.exception(e)
                        ingest_attempt += 1
                        ingest_error = str(e)

                        sql = "UPDATE citations_registration_queue SET ingest_attempts=%d WHERE id = %d ;" % (
                        ingest_attempt, citation_object_id)
                        try:
                            csr.execute(sql)

                        except psycopg2.DatabaseError as e:
                            message = 'Database error! ' + e
                            self._L.exception('Operational error!\n{0}')
                            self._L.exception(e)

                        except psycopg2.OperationalError as e:
                            self._L.exception('Operational error!\n{0}')
                            self._L.exception(e)

                    finally:
                        self.conn.commit()
            except Exception as e:
                self._L.exception('Exception! \n{0}')
                self._L.exception(e)
                print("Exception occured  : " + str(e))
                continue

        return


    def escapeSolrQueryTerm(self, term):
        term = term.replace('\\', '\\\\')
        for c in SOLR_RESERVED_CHAR_LIST:
            term = term.replace(c, '\{}'.format(c))
        return term


    def getExistingCitationsData(self):
        """
        Retrieves existing citations from the database
        :return:
        """
        csr = self.getCursor()
        getAllIDsSQL = "select target_id, source_id from CITATIONS;"
        csr.execute(getAllIDsSQL)
        data = csr.fetchall()
        citations_dict = {}
        for tup in data:
            if tup[0] not in citations_dict:
                citations_dict[tup[0]] = []
            citations_dict[tup[0]].append(tup[1])

        return citations_dict


    def validateUniqueCitation(self, given_target_id, given_source_id, citations_dict):
        """
        Validates that the citation is unique and does not already exist in the database
        :param given_target_id:
        :param given_source_id:
        :param retrieveData:
        :return:
        """

        if citations_dict is None:
            citations_dict = self.getExistingCitationsData()

        if len(given_target_id) > 0 and given_target_id.find("10.") > -1:
            target_doi_start = given_target_id.index("10.")
            given_target_doi = given_target_id[target_doi_start:]
        else:
            given_target_doi = given_target_id

        if len(given_source_id) > 0 and given_source_id.find("10.") > -1:
            source_doi_start = given_source_id.index("10.")
            given_source_doi = given_source_id[source_doi_start:]
        else:
            given_source_doi = given_source_id

        if given_target_id in citations_dict or given_target_doi in citations_dict:
            if given_target_id in citations_dict:
                if given_source_id in citations_dict[given_target_id] or \
                        given_source_doi in citations_dict[given_target_id]:
                    self._L.info(
                        given_target_doi + " has already a relation with " + given_source_doi)
                    return False
            if given_target_doi in citations_dict:
                if given_source_id in citations_dict[given_target_doi] or \
                        given_source_doi in citations_dict[given_target_doi]:
                    self._L.info(
                        given_target_doi + " has already a relation with " + given_source_doi)
                    return False
        return True


if __name__ == "__main__":
    md = MetricsDatabase()
    md.logConfig("metrics_database.log","%(name)s - %(levelname)s - %(message)s", "INFO")
    # md.parseCitationsFromDisk("abs_cit_part_1_complete.json")
    # md.insertCitationObjects(read_citations_from_file="abs_cit_part_1_complete.json")
    # md.parseCitationsFromDisk("Springer.json")

    # md.getDOIs()
    # md.queueCitationRequest(req)
    # md.parseQueuedCitationRequests()
    # citation_object = {
    #   "request_type": "dataset",
    #   "metadata":
    #   [
    #     {
    #         "target_id" : "DOI:10.5060/DATAONEDOI3",
    #         "source_id" : "DOI:10.1002/ppp.695",
    #         "relation_type" : "isCitedBy"
    #     }
    #   ]
    # }
    # md.processCitationQueueObject(citation_object)
