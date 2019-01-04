'''
Manage the log event Elastic Search service.

Requires that port 9200 is connected to the elastic search instance, e.g.:

   ssh -L9200:localhost:9200 logproc-stage-ucsb-1.test.dataone.org

Example, show events for last seven days:

  d1metricses -c localconfig.ini -l events -S "7 days ago"

Example, sessions with 1000 or more events::

  d1metricses -c localconfig.ini  -l sessions -m 1000

'''


import argparse
import json
import logging
import os
import sys
from d1_metrics import common
from d1_metrics import metricselasticsearch


def esCheck(args):
  '''
  Dump some stats about the ES instance
  Args:
    args:

  Returns:

  '''
  #Get a logger with the current method name
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  elastic = metricselasticsearch.MetricsElasticSearch(args.config)
  elastic.connect()
  print(json.dumps(elastic.getInfo(show_mappings=args.verbose), indent=2))
  return 0


def esInitialize(args):
  #Get a logger with the current method name
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  return 0


def esProcessEvents(args):
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  elastic = metricselasticsearch.MetricsElasticSearch(args.config)
  elastic.connect()

  return 0


def getDateStartEnd(args):
  d_start = None
  d_end = None
  if args.datestart is not None:
    d_start = common.textToDateTime(args.datestart)
    logging.info("Start date parsed as: %s", d_start.isoformat())
  if args.dateend is not None:
    d_end = common.textToDateTime(args.dateend)
    logging.info("End date parsed as: %s", d_end.isoformat())
  return d_start, d_end


def esGetEvents(args):
  '''
  Query the ES instance for events (default=read)

  Returns logevents that have
    beat.name = eventlog
    formatType = "data"
    have a sessionid
    and event is "read"

  Args:
    args: the parsed arguments from ArgumentParser

  Returns:
    exit code
  '''
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  d_start, d_end = getDateStartEnd(args)
  elastic = metricselasticsearch.MetricsElasticSearch(args.config)
  elastic.connect()
  fields = None
  if args.fields is not None:
    fields = args.fields.split(",")
    fields = [field.strip() for field in fields]
  events, nhits = elastic.getEvents(limit=args.limit, date_start=d_start, date_end=d_end, fields=fields)
  print("Numer of hits: {}".format(nhits))
  print(json.dumps(events, indent=2))
  return 0


def esGetSearches(args):
  '''
  Query the ES instance for "searches"

  By default this issues a query looking for:
     log events
     from the search beat
     with a sessionid
     and message matches "/cn/v2/query/solr/"

  Args:
    args: the parsed arguments from ArgumentParser

  Returns:
    exit code

  '''
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  d_start, d_end = getDateStartEnd(args)
  elastic = metricselasticsearch.MetricsElasticSearch(args.config)
  elastic.connect()
  events, nhits = elastic.getSearches(limit=args.limit, date_start=d_start, date_end=d_end)
  print("Numer of hits: {}".format(nhits))
  print(json.dumps(events, indent=2))
  return 0


def esGetSessions(args):
  '''

  Args:
    args:

  Returns:

  '''
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  d_start, d_end = getDateStartEnd(args)
  elastic = metricselasticsearch.MetricsElasticSearch(args.config)
  elastic.connect()
  sessions, nsessions = elastic.getSessions(limit=args.limit, date_start=d_start, date_end=d_end, min_aggs=args.minaggs)
  print("Number of sessions matching request: {}".format(nsessions))
  print("SessionId   Count                   Start-time                      End-time    d-min")
  for session in sessions:
    row = [0,0,0,0,0]
    dt = session[3] - session[2]
    row[0] = session[0]
    row[1] = session[1]
    row[2] = session[2].isoformat()
    row[3] = session[3].isoformat()
    row[4] = dt.days + dt.seconds/(24*60*60)
    row[4] = row[4] * (24*60)
    print("{0:>9}{1:>8}{2:>30}{3:>30}{4:8.2f}".format(*row))


def esComputeSessions(args):
  '''
  Compute session information for events.
  Args:
    args:

  Returns:

  '''
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  elastic = metricselasticsearch.MetricsElasticSearch(args.config)
  elastic.connect()
  elastic.computeSessions(dry_run=args.dryrun)


def main():
  commands = {
    "check":esCheck,
    "initialize":esInitialize,
    "events": esGetEvents,
    "searches": esGetSearches,
    "sessions": esGetSessions,
    "compute": esComputeSessions,
  }
  parser = argparse.ArgumentParser(description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('-l', '--log_level',
                      action='count',
                      default=0,
                      help='Set logging level, multiples for more detailed.')
  parser.add_argument("-c", "--config",
                      default=common.DEFAULT_CONFIG_FILE,
                      help="Configuration file.")
  parser.add_argument("-V", "--verbose",
                      default=False,
                      action="store_true",
                      help="Be verbose")
  parser.add_argument("-L","--limit",
                      default=10,
                      type=int,
                      help="Number of rows to return for queries (10)")
  parser.add_argument("-S","--datestart",
                      default=None,
                      help="Specify date for start of records to retrieve")
  parser.add_argument("-E","--dateend",
                      default=None,
                      help="Specify date for end of records to retrieve")
  parser.add_argument("-F","--fields",
                      default="*",
                      help="Specify fields to show")
  parser.add_argument("-m","--minaggs",
                      default=1,
                      help="Minimum number of aggregatd values in sessions to return (1)")
  parser.add_argument("-Y","--dryrun",
                      action="store_true",
                      help="Dry run - don't make any changes.")
  parser.add_argument('command',
                      nargs='?',
                      default="check",
                      help="Operation to perform ({})".format(", ".join(commands.keys())))
  args = parser.parse_args()
  # Setup logging verbosity
  levels = [logging.WARNING, logging.INFO, logging.DEBUG]
  level = levels[min(len(levels) - 1, args.log_level)]
  logging.basicConfig(level=level,
                      format="%(asctime)s %(name)s %(levelname)s: %(message)s")

  if (args.command) not in commands.keys():
    logging.error("Unknown command: %s", args.command)
    return 1
  if not os.path.exists(args.config):
    logging.error("Configuration file not found: %s", args.config)
    return 1
  return commands[args.command](args)


if __name__ == "__main__":
  sys.exit(main())