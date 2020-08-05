"""
Uses the solr index to provide identifier resolution for packages, obsolescence, versions.

IMPORTANT: Requires Python 3.5 or later
"""

import sys
import time
import logging
import requests
import json
import asyncio
from aiohttp import ClientSession
from d1_metrics.solrclient import SolrClient
import concurrent.futures


PRODUCTION_SOLR = "https://cn.dataone.org/cn/v2/query/solr/"
DEFAULT_SOLR = PRODUCTION_SOLR
CONCURRENT_REQUESTS = 20  #max number of concurrent requests to run

# List of characters that should be escaped in solr query terms
SOLR_RESERVED_CHAR_LIST = [
  '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':'
  ]


def _getLogger():
  '''
  Return a logger and a timestamp to help with profiling.

  Returns: instance of logging.logger, timestamp
  '''
  logger = logging.getLogger()
  return logger, time.time()


def escapeSolrQueryTerm(term):
  '''
  Escape a solr query term for solr reserved characters
  Args:
    term: query term to be escaped

  Returns: string, the escaped query term
  '''
  term = term.replace('\\', '\\\\')
  for c in SOLR_RESERVED_CHAR_LIST:
    term = term.replace(c, '\{}'.format(c))
  return term


def quoteTerm(term):
  '''
  Return a quoted, escaped Solr query term
  Args:
    term: (string) term to be escaped and quoted

  Returns: (string) quoted, escaped term
  '''
  return '"' + escapeSolrQueryTerm(term) + '"'


def _defaults(solr_url):
  '''
  Get default for Solr endpoint

  Args:
    solr_url: None or URL

  Returns: solr_url
  '''
  if solr_url is None:
    solr_url = DEFAULT_SOLR
  return solr_url


def _getIdsFromSolrResponse(response_text, pids=[]):
  '''
  Helper to retrieve identifiers from the solr response

  Args:
    response_text: The solr response json text.
    pids: A list of identifiers to which identifiers here are added

  Returns: pids with any additional identifiers appended.
  '''
  data = json.loads(response_text)
  for doc in data['response']['docs']:
    try:
      pid = doc['id']
      if not pid in pids:
        pids.append(pid)
    except KeyError as e:
      pass
    try:
      for pid in doc['documents']:
        if not pid in pids:
          pids.append(pid)
    except KeyError as e:
      pass
    try:
      pid = doc['obsoletes']
      if not pid in pids:
        pids.append(pid)
    except KeyError as e:
      pass
    try:
      for pid in doc['resourceMap']:
        if not pid in pids:
          pids.append(pid)
    except KeyError as e:
      pass
    # Added parsing for series identifiers
    try:
      pid = doc['seriesId']
      if not pid in pids:
        pids.append(pid)
    except KeyError as e:
      pass
  return pids


def pidsAndSid(IDs, solr_url=None):
  '''
  For each provided ID, determine if it is a PID or a SID

  If id is a SID, then all the associated PIDs are returned.

  If id is a PID with a SID, then the SID and associated PIDs are
  returned.

  There is no particular ordering to the PIDs.

  Uses the Solr index for the lookup. Not that this will currently
  not work for archived content.

  Args:
    an_id: identifier to lookup
    solr_url: the Solr select URL or None

  Returns: [{'id':an_id, 'is_sid': boolean, 'pids':[PID, ...], 'sid':SID}, ...]
  '''

  async def _fetch(an_id, session):
    url = DEFAULT_SOLR
    params = {'wt': 'json',
              'fl': 'id,seriesId',
              }
    query = quoteTerm(an_id)
    params['q'] = 'id:' + query + ' OR seriesId:' + query

    async with session.get(url, params=params) as response:
      response_text = await response.text()
      result = {'id': an_id,
                'pids': [],
                'sid': None,
                'is_sid': False}
      if response.status == 200:
        data = json.loads(response_text)
        pid_set = set()
        sid_set = set()
        for doc in data['response']['docs']:
          try:
            sid_set.add(doc['seriesId'])
          except KeyError as e:
            pass
          # must always be an id
          pid_set.add(doc['id'])
        if an_id in sid_set:
          result['is_sid'] = True
        if len(sid_set) > 1:
          _L.error("Whoa, more than one SID found for an_id: %s", an_id)
        result['pids'] = list(pid_set)
        try:
          result['sid'] = sid_set.pop()
        except KeyError as e:
          pass
          _L.debug("No sid for an_id: %s", an_id)
      return result

  async def _work(loop, pids):
    tasks = []
    async with ClientSession(loop=loop) as session:
      for pid in pids:
        tasks.append( asyncio.ensure_future(_fetch(pid, session)) )
      responses = await asyncio.gather(*tasks)
      return responses

  _L, t_0 = _getLogger()
  _L.debug("Enter")
  _L.debug("Resolving %d identifiers", len(IDs))
  try:
    loop = asyncio.get_event_loop()
  except RuntimeError as e:
    _L.info("Creating new event loop.")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
  future = asyncio.ensure_future( _work(loop, IDs) )
  results = loop.run_until_complete( future )
  _L.debug("elapsed:%fsec", time.time()-t_0)
  return results


def getObsolescenceChain(IDs, solr_url=None, max_depth=20):
  '''
  Get the obsolecence chains for pids in IDs

  Given a pid, get the obsolescence chain, that is, any pid that this pid obsoletes,
  and so forth until no more pids in the chain.

  This method may make multiple calls to the solr endpoint to find all the
  obsoleted objects.

  Args:
    solr_url: URL for the solr select endpoint
    pids: list of identifiers
    max_depth: Maximum length of obsolescence chain to explore, for self preservation

  Returns: {ID: [pids], ...}

  '''

  def _fetch(an_id):
    url = DEFAULT_SOLR
    session = requests.Session()
    params = {'wt':'json',
              'fl':'obsoletes',
              'q.op':'OR',
              }
    obsoleted_pids = [an_id, ]
    more_work = True
    depth = 1
    while more_work and depth < max_depth:
      # query for any of the ids as PID or SID
      query = quoteTerm(obsoleted_pids[-1])
      params['q'] = "id:" + query + " OR seriesId:" + query
      response = session.get(url, params=params)
      response_text = response.text
      if response.status_code == 200:
        data = json.loads(response_text)
        if len(data['response']['docs']) > 1:
          _L.warning("More than a single obsoleted entry returned for pid: %s", obsoleted_pids[-1])
        try:
          obsoleted_pids.append(data['response']['docs'][0]['obsoletes'])
          depth += 1
        except IndexError as e:
          more_work = False
        except KeyError as e:
          more_work = False
      else:
        more_work = False
      if depth >= max_depth:
        _L.warning("Recursion limit hit for PID: %s", an_id)
        more_work = False
    return obsoleted_pids

  async def _work(loop, pids):
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
      loop = asyncio.get_event_loop()
      tasks = []
      for an_id in pids:
        tasks.append( loop.run_in_executor( executor, _fetch, an_id ))
    for response in await asyncio.gather( *tasks ):
      results[response[0]] = response[1:]
    return results

  _L, t_0 = _getLogger()
  _L.debug("Enter")
  try:
    loop = asyncio.get_event_loop()
  except RuntimeError as e:
    _L.info("Creating new event loop.")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
  future = asyncio.ensure_future( _work(loop, IDs) )
  results = loop.run_until_complete( future )
  _L.debug("elapsed:%fsec", time.time()-t_0)
  return results


def getResolvePIDs(PIDs, solr_url=None, use_mm_params=True):
  '''
  Implements same functionality as metricsreader.resolvePIDs, except works asynchronously for input pids

  input: ["urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863", ]
  output: {"urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863": ["urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863",
                                                             "knb.92123.1",
                                                             "urn:uuid:d64e5f8b-c91c-487a-8ce7-0cd271194f34",
                                                             "urn:uuid:bb01b2c8-5e6c-4645-903d-39dbdd8d4d56",
                                                             "urn:uuid:d80dc5c2-bfd7-4023-87a3-9e47a2c57fbb",
                                                             "urn:uuid:9609acb1-63f2-40c6-88e3-ca9a16b06c79",
                                                             "urn:uuid:542141d3-ed5a-4d97-b759-28a17757b0b8",
                                                             "urn:uuid:22ef5022-8ade-4549-acac-c18656dd2033",
                                                             "urn:uuid:2cdf8adb-79c4-4b6c-875a-3e459c3817c7"],
          }
  Args:
    PIDs:
    solr_url:

  Returns:
  '''

  def _doPost(session, url, params, use_mm=True):
    """
    Post a request, using mime-multipart or not. This is necessary because
    calling solr on the local address on the CN bypasses the CN service interface which
    uses mime-multipart requests.

    Args:
      session: Session instance
      url: URL for request
      params: params configure for mime-multipart request
      use_mm: if not true, then a form request is made.

    Returns: response object
    """
    if use_mm:
      return session.post(url, files=params)
    paramsd = {key:value[1] for (key,value) in params.items()}
    # This is necessary because the default query is set by the DataONE SOLR connector
    # which is used when accessing solr through the public interface.
    if not 'q' in paramsd:
      paramsd['q'] = "*:*"
    return session.post(url, data=paramsd)


  def _fetch(url, an_id):
    session = requests.Session()
    resMap = []
    result = []
    #always return at least this identifier
    result.append(an_id)

    # including the very first pid in resourceMap
    resMap.append(an_id)

    params = {'wt':(None,'json'),
              'fl':(None,'documents,resourceMap'),
              'rows':(None,1000)
              }
    params['fq'] = (None,"((id:" + quoteTerm(an_id) + ") OR (seriesId:" + quoteTerm(an_id) + "))")
    response = _doPost(session, url, params, use_mm=use_mm_params)
    if response.status_code == requests.codes.ok:
      #continue
      logging.debug(response.text)
      resMap = _getIdsFromSolrResponse(response.text,resMap)
      more_resMap_work = True
      params['fl'] = (None,'documents,obsoletes')

      while more_resMap_work:
        current_length = len(resMap)
        query = ") OR (".join(map(quoteTerm, resMap))
        params['fq'] = (None,"id:((" + query + "))")
        response = _doPost(session, url, params, use_mm=use_mm_params)
        if response.status_code == requests.codes.ok:
          resMap = _getIdsFromSolrResponse(response.text, resMap)
          if len(resMap) == current_length:
            more_resMap_work = False
        else:
          more_resMap_work = False

      # Adding the resMap identifiers to the datasetIdentifierFamily
      result.extend(resMap)

      params['fl'] = (None, 'id,seriesId,documents,obsoletes')
      query = ") OR (".join(map(quoteTerm, resMap))
      params['fq'] = (None,"resourceMap:((" + query + "))")
      response = _doPost(session, url, params, use_mm=use_mm_params)
      if response.status_code == requests.codes.ok:
        result = _getIdsFromSolrResponse(response.text, result)

      more_work = True
      while more_work:
        current_length = len(result)
        query = ") OR (".join( map(quoteTerm, result) )
        params['fq'] = (None,'id:((' + query + '))')
        response = _doPost(session, url, params, use_mm=use_mm_params)
        if response.status_code == requests.codes.ok:
          result = _getIdsFromSolrResponse(response.text, result)
          if len(result) == current_length:
            more_work = False
        else:
          more_work = False
    return result

  async def _work(pids):
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
      loop = asyncio.get_event_loop()
      tasks = []
      for an_id in pids:
        url = _defaults(solr_url) #call here as option for RR select
        tasks.append(loop.run_in_executor(executor, _fetch, url, an_id ))
      for response in await asyncio.gather(*tasks):
        results[ response[0] ] = response

  _L, t_0 = _getLogger()
  results = {}
  _L.debug("Enter")
  # In a multithreading environment such as under gunicorn, the new thread created by
  # gevent may not provide an event loop. Create a new one if necessary.
  try:
    loop = asyncio.get_event_loop()
  except RuntimeError as e:
    _L.info("Creating new event loop.")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

  future = asyncio.ensure_future(_work(PIDs))
  loop.run_until_complete( future )
  _L.debug("elapsed:%fsec", time.time()-t_0)
  return results


#####################################
# Using the SOLR client to get the portals colletion query
#
#####################################


def getPortalCollectionQueryFromSolr(url = None, portalLabel = None):
    """
      Returns the collection query for the portal
      :param: url
      :param: portalLabel
      :param: pid
      :param: seriesId

      :returns:
        CollectionQuery string retrieved from SOLR
    """
    _L, t_0 = _getLogger()
    try:
      if url is None:
        url = "https://cn.dataone.org/cn/v2/query"

      # Create a solr client object
      solrClientObject = SolrClient(url, "solr")
      solrQuery = "*:*"

      # supports retrieval of collection query via portal label, seriesId and PID
      if (portalLabel is not None):
        solrQuery = "(-obsoletedBy:* AND (label:" + portalLabel + ") OR (seriesId:" + portalLabel + ") OR (id:" + portalLabel + "))"

      data = solrClientObject.getFieldValues('collectionQuery', q=solrQuery)
      colelctionQuery = data["collectionQuery"][0]
    except Exception as e:
      colelctionQuery = None
      _L.error(e)

    return colelctionQuery


def resolveCollectionQueryFromSolr(url = None, collectionQuery = "*:*"):
  """
    Uses d1_metrics SolrClient to resolve a collection Query.
    :param: url
    :param: collection query

    :returns:
      resolved_collection_identifier: array object of the resolved collection identifiers
  """
  _L, t_0 = _getLogger()
  # Point to the CN SOLR endpoint by default
  if url is None:
    url = "https://cn.dataone.org/cn/v2/query"

  # unescape the escaped characters
  collectionQuery = string_escape(collectionQuery)

  # Create a solr client object
  solrClientObject = SolrClient(url, "solr")
  resolved_collection_identifier = []

  try:
    _L.debug("Fetching collections PIDs")
    data = solrClientObject.getFieldValues('id', q=collectionQuery)

    for hit in data['id'][::2]:
      resolved_collection_identifier.append(hit)
  except:
    _L.error("Error in fetching collection PIDs")

  return resolved_collection_identifier


def getResolvedTargetCitationMetadata(PIDs, solr_url=None, use_mm_params=True):
  '''
  Resolves the Citaion metadata for target identifier

  input: ["urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863", ]
  output: {"10.5063/F1K935SB": {
                        "id": "doi:10.5063/F1K935SB",
                        "origin": [
                            "Meagan Krupa",
                            "Molly Cunfer",
                            "Jeanette Clark"
                        ],
                        "title": "Alaska Board of Fisheries Proposals 1959-2016",
                        "datePublished": "2017-10-19T00:00:00Z",
                        "dateUploaded": "2018-11-05T22:46:28.946Z",
                        "dateModified": "2018-11-05T22:46:32.04Z"
                    },
          }
  Args:
    PIDs:
    solr_url:

  Returns:
  '''

  def _doPost(session, url, params, use_mm=True):
    """
    Post a request, using mime-multipart or not. This is necessary because
    calling solr on the local address on the CN bypasses the CN service interface which
    uses mime-multipart requests.

    Args:
      session: Session instance
      url: URL for request
      params: params configure for mime-multipart request
      use_mm: if not true, then a form request is made.

    Returns: response object
    """
    if use_mm:
      return session.post(url, files=params)
    paramsd = {key: value[1] for (key, value) in params.items()}
    # This is necessary because the default query is set by the DataONE SOLR connector
    # which is used when accessing solr through the public interface.
    if not 'q' in paramsd:
      paramsd['q'] = "*:*"
    return session.post(url, data=paramsd)

  def _fetch(url, an_id):
    session = requests.Session()
    result = {}
    # always return at least this identifier
    result["id"] = an_id
    result["metadata"] = {}

    params = {'wt': (None, 'json'),
              'fl': (None, 'id,seriesId,origin,title,datePublished,dateUploaded,dateModified'),
              'rows': (None, 1000)
              }
    query_string = "((id:*" + an_id.lower() + "*) OR (id:*" + an_id.upper() + \
                            "*) OR (id:*" + an_id + "*) OR (seriesId:*" + an_id + \
                            "*) OR (seriesId:*" + an_id.lower() + "*) OR (seriesId:*" + an_id.upper() + "*))"
    params['fq'] = (None, query_string)

    response = _doPost(session, url, params, use_mm=use_mm_params)
    if response.status_code == requests.codes.ok:
      # continue
      logging.debug(response.text)
      data = json.loads(response.text)
      if data["response"]["numFound"] > 0:

        if "id" in data["response"]["docs"][0]:
          result["metadata"]["id"] = data["response"]["docs"][0]["id"]

        if "seriesId" in data["response"]["docs"][0]:
          result["metadata"]["seriesId"] = data["response"]["docs"][0]["seriesId"]

        if "origin" in data["response"]["docs"][0]:
          result["metadata"]["origin"] = data["response"]["docs"][0]["origin"]

        if "title" in data["response"]["docs"][0]:
          result["metadata"]["title"] = data["response"]["docs"][0]["title"]

        if "datePublished" in data["response"]["docs"][0]:
          result["metadata"]["datePublished"] = data["response"]["docs"][0]["datePublished"]

        if "dateUploaded" in data["response"]["docs"][0]:
          result["metadata"]["dateUploaded"] = data["response"]["docs"][0]["dateUploaded"]

        if "dateModified" in data["response"]["docs"][0]:
          result["metadata"]["dateModified"] = data["response"]["docs"][0]["dateModified"]
    return result

  async def _work(pids):
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
      loop = asyncio.get_event_loop()
      tasks = []
      for an_id in pids:
        url = _defaults(solr_url)  # call here as option for RR select
        tasks.append(loop.run_in_executor(executor, _fetch, url, an_id))
      for response in await asyncio.gather(*tasks):
        results[response["id"]] = response["metadata"]

  _L, t_0 = _getLogger()
  results = {}
  _L.debug("Enter")
  # In a multithreading environment such as under gunicorn, the new thread created by
  # gevent may not provide an event loop. Create a new one if necessary.
  try:
    loop = asyncio.get_event_loop()
  except RuntimeError as e:
    _L.info("Creating new event loop.")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

  future = asyncio.ensure_future(_work(PIDs))
  loop.run_until_complete(future)
  _L.debug("elapsed:%fsec", time.time() - t_0)
  return results


def string_escape(s, encoding='utf-8'):
  """
    Un-escaping the escaped strings

      :param: s String to be unescaped
      :param: 

      :returns: Unescaped String
  """
  return (s.encode('latin1')         # To bytes, required by 'unicode-escape'
            .decode('unicode-escape') # Perform the actual octal-escaping decode
            .encode('latin1')         # 1:1 mapping back to bytes
            .decode(encoding))        # Decode original encoding


if __name__ == "__main__":
  from pprint import pprint

  def eg_getObsolescenceChain():
    pids = ['{06EB6E16-A235-443A-8D46-5AFFD4705875}',
            '{A16FBF01-D933-435F-8703-74D50C22DAD9}',
            '{95C011DB-3654-4497-BC26-5BB6F4E4AB3D}',
            'bogus']
    obsoletes = [['{D9F51B15-034A-45CE-8058-015E44E3545B}',
                  '{832B57C9-894F-49C7-B60C-E507B1644AEA}',
                  '{A428F062-E3CC-4B05-990D-DA599936C85D}'],
                 ['{A8EFA92C-C780-488C-88CC-F87AD93D693E}',
                  '{9E46484A-7E5A-4CA7-BF24-5E006BA00236}',
                  '{A1D0D014-2D43-46D9-AE21-5808EB5C41E5}'],
                 ['{8D27C588-15A0-48F8-8A44-424E3344072C}'], ]
    res = getObsolescenceChain(pids + obsoletes[0] + obsoletes[1] + obsoletes[2])
    pprint(res, indent=2)


  def eg_pidsAndSid():
    sids = ["doi:10.1594/PANGAEA.855486",
            "391822_format=d1rem",
            "doi:10.5065/D6M9071Z"]
    pids = ["9efa6e0b54cff15f2f8e0b3272e36375",
            "doi:10.6067:XCV8Z89FKW_format=d1rem1495494142691",
            "urn:uuid:779db71a-16cf-4ae1-8ba3-bdc0b520d39f"]
    res = pidsAndSid(sids + pids)
    pprint(res, indent=2)


  def eg_getResolvePids():
    pids = ["urn:uuid:f46dafac-91e4-4f5f-aaff-b53eab9fe863", ]
    res = getResolvePIDs(pids, "http://localhost:8983/solr/search_core/select", use_mm_params=False)
    #res = getResolvePIDs(pids)
    pprint(res, indent=2)


  #change verbosity of the urllib3.connectionpool logging
  #logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
  logging.basicConfig(level=logging.DEBUG, format='%(threadName)10s %(name)18s: %(message)s')
  # eg_pidsAndSid()
  # eg_getObsolescenceChain()
  # print("==eg: getResolvePids==")
  # eg_getResolvePids()
  # getPortalCollectionQuery(url="https://dev.nceas.ucsb.edu/knb/d1/mn/v2/query/solr/?", portalLabel="portal-test")
