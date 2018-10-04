'''
Implements a python iterator for iterating over entries of a solr search response.
'''

import time
import urllib
import logging
import requests
import json

NINE_BILLION = 9999999999
DEFAULT_PAGE_SIZE = 10000
RESULTS_WARN_LEVEL = 100001
APP_LOG_NAME = "app"
SOLR_RESERVED_CHAR_LIST = [
  '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*',
  '?', ':'
]


def escapeSolrQueryTerm(term):
  term = term.replace('\\', '\\\\')
  for c in SOLR_RESERVED_CHAR_LIST:
    term = term.replace(c, '\{}'.format(c))
  return term


class SolrSearchResponseIterator(object):
  '''
  Performs a search against a Solr index and acts as an iterator to retrieve
  all the values.
  '''

  def __init__(self,
               select_url,
               q,
               fq=None,
               fields='*',
               page_size=DEFAULT_PAGE_SIZE,
               max_records=None,
               sort=None,
               **query_args):
    self.logger = logging.getLogger(APP_LOG_NAME)
    self.client = requests.Session()
    self.select_url = select_url
    self.q = q
    self.fq = fq
    self.fields = fields
    self.query_args = query_args
    if max_records is None:
      max_records = NINE_BILLION
    self.max_records = max_records
    self.sort = sort
    self.c_record = 0
    self.page_size = page_size
    self.res = None
    self.done = False
    self._next_page(self.c_record)
    self._num_hits = 0
    if self.res['response']['numFound'] > RESULTS_WARN_LEVEL:
      self.logger.warning("Retrieving %d records...", self.res['response']['numFound'])


  def _next_page(self, offset):
    '''
    Retrieves the next set of results from the service.
    '''
    self.logger.debug("Iterator c_record=%d", self.c_record)
    start_time = time.time()
    page_size = self.page_size
    if (offset + page_size) > self.max_records:
      page_size = self.max_records - offset
    query_dict = {
      'q': self.q,
      'start': str(offset),
      'rows': str(page_size),
      'fl': self.fields,
      'wt': 'json',
    }
    if self.fq is not None:
      query_dict['fq'] = self.fq
    if self.sort is not None:
      query_dict['sort'] = self.sort
    params = urllib.parse.urlencode(query_dict) #, quote_via=urllib.parse.quote)
    self.logger.debug("request params = %s", str(params))
    response = self.client.get(self.select_url, params=params)
    self.res = json.loads(response.text)
    self._num_hits = int(self.res['response']['numFound'])
    end_time = time.time()
    self.logger.debug("Page loaded in %.4f seconds.", end_time - start_time)


  def process_row(self, row):
    '''
    Override this method in derived classes to transform the row response.
    Args:
      row: a row from the solr search response

    Returns:
      row, adjusted
    '''
    return row


  def __iter__(self):
    return self


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
    return self.process_row( row )

