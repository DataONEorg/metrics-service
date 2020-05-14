import os
import argparse
import logging
import logging.handlers
import urllib.request, urllib.parse, urllib.error
import requests
import datetime
import json
import re
import time
from configparser import ConfigParser


LOG_NAME = "logagg"
APP_LOG = "app"
PAGE_SIZE = 10000 #Number of records to reetrieve per request
DEFAULT_CORE = "event_core" #name of the solr core to query
MAX_LOGFILE_SIZE = 1073741824 #1GB


#========================
#==== Client implementing an iterator for paging over Solr results

SOLR_RESERVED_CHAR_LIST = [
  '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*',
  '?', ':'
]


def escapeSolrQueryTerm(term):
  term = term.replace('\\', '\\\\')
  for c in SOLR_RESERVED_CHAR_LIST:
    term = term.replace(c, '\{}'.format(c))
  return term


class SolrClient(object):

  def __init__(self, base_url, core_name, select="/"):
    self.base_url = base_url
    self.core_name = core_name
    self._select = select
    self.logger = logging.getLogger(APP_LOG)
    self.client = requests.Session()
    self.cert = self._getSolrCert()


  def _getSolrCert(self):
    '''
    Returns a solr certificate file for the queries

    :return: file certificate  object to query solr
    '''
    parser = ConfigParser()

    found_files = parser.read(os.path.join(os.path.dirname(__file__), './../../../', 'localconfig.ini'))

    return(parser.get("solr_config", "cert"))


  def doGet(self, params):
    params['wt'] = 'json'
    url = self.base_url + "/" + self.core_name + self._select
    response = self.client.get(url, params=params, cert=self.cert)
    data = json.loads(response.text)
    return data


  def getFieldValues(self, name,
                      q='*:*',
                      fq=None,
                      maxvalues=-1,
                      sort=True,
                      **query_args):
    """Retrieve the unique values for a field, along with their usage counts.
    :param sort: Sort the result
    :param name: Name of field for which to retrieve values
    :type name: string
    :param q: Query identifying the records from which values will be retrieved
    :type q: string
    :param fq: Filter query restricting operation of query
    :type fq: string
    :param maxvalues: Maximum number of values to retrieve. Default is -1,
      which causes retrieval of all values.
    :type maxvalues: int
    :returns: dict of {fieldname: [[value, count], ... ], }
    """
    params = {
      'q': q,
      'rows': '0',
      'facet': 'true',
      'facet.field': name,
      'facet.limit': str(maxvalues),
      'facet.zeros': 'false',
      'facet.sort': str(sort).lower(),
      'fq': fq,
    }
    resp_dict = self.doGet(params)
    result_dict = resp_dict['facet_counts']['facet_fields']
    result_dict['numFound'] = resp_dict['response']['numFound']
    return result_dict


class SolrSearchResponseIterator(SolrClient):
  """Performs a search against a Solr index and acts as an iterator to retrieve
  all the values."""

  def __init__(self, base_url, core_name, q, select="select", fq=None, fields='*', page_size=PAGE_SIZE, max_records=None, sort=None, **query_args):
    super(SolrSearchResponseIterator, self).__init__(base_url, core_name, select=select)
    self.q = q
    self.fq = fq
    self.fields = fields
    self.query_args = query_args
    if max_records is None:
      max_records = 9999999999
    self.max_records = max_records
    self.sort = sort
    self.c_record = 0
    self.page_size = page_size
    self.res = None
    self.done = False
    self._next_page(self.c_record)
    self._num_hits = 0
    if self.res['response']['numFound'] > 1000:
      self.logger.warning("Retrieving %d records...", self.res['response']['numFound'])


  def _next_page(self, offset):
    """Retrieves the next set of results from the service."""
    self.logger.debug("Iterator c_record=%d", self.c_record)
    start_time = time.time()
    page_size = self.page_size
    if (offset + page_size) > self.max_records:
      page_size = self.max_records - offset
    params = {
      'q': self.q,
      'start': str(offset),
      'rows': str(page_size),
      'fl': self.fields,
      'wt': 'json',
    }
    if self.fq is not None:
      params['fq'] = self.fq
    if self.sort is not None:
      params['sort'] = self.sort
    #params = urllib.parse.urlencode(query_dict) #, quote_via=urllib.parse.quote)
    self.logger.debug("request params = %s", str(params))
    self.res = self.doGet(params=params)
    #self.res = json.loads(response.text)
    self._num_hits = int(self.res['response']['numFound'])
    end_time = time.time()
    self.logger.debug("Page loaded in %.4f seconds.", end_time - start_time)

  def __iter__(self):
    return self


  def process_row(self, row):
    """Override this method in derived classes to reformat the row response."""
    return row


  def __next__(self):
    if self.done:
      raise StopIteration()
    if self.c_record > self.max_records:
      self.done = True
      raise StopIteration()
    idx = self.c_record - self.res['response']['start']
    try:
      row = self.res['response']['docs'][idx]
    except IndexError:
      self._next_page(self.c_record)
      idx = self.c_record - self.res['response']['start']
      try:
        row = self.res['response']['docs'][idx]
      except IndexError:
        self.done = True
        raise StopIteration()
    self.c_record = self.c_record + 1
    return row


if __name__ == "__main__":
  sc = SolrClient("cn/solr", "solr")
  print(sc.cert)