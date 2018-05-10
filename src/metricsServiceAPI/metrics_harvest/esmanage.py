'''
Manage the log event Elastic Search service.


'''


import sys
import os
import argparse
import logging
import json
import metrics_common
from . import metricsharvest


def esCheck(args):
  '''
  Dump some stats about the ES instance
  Args:
    args:

  Returns:

  '''
  #Get a logger with the current method name
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  elastic = metricsharvest.ElasticMetricLog(args.config)
  elastic.connect()
  print(json.dumps(elastic.getInfo(show_mappings=args.verbose), indent=2))
  return 0


def esInitialize(args):
  #Get a logger with the current method name
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  return 0


def esProcessEvents(args):
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  elastic = metricsharvest.ElasticMetricLog(args.config)
  elastic.connect()

  return 0


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
  elastic = metricsharvest.ElasticMetricLog(args.config)
  elastic.connect()
  events = elastic.getEvents(limit=args.limit)
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
  elastic = metricsharvest.ElasticMetricLog(args.config)
  elastic.connect()
  events = elastic.getSearches(limit=args.limit)
  print(json.dumps(events, indent=2))
  return 0


def main():
  commands = {
    "check":esCheck,
    "initialize":esInitialize,
    "events": esGetEvents,
    "searches": esGetSearches,
  }
  parser = argparse.ArgumentParser(description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('-l', '--log_level',
                      action='count',
                      default=0,
                      help='Set logging level, multiples for more detailed.')
  parser.add_argument("-c", "--config",
                      default=metrics_common.DEFAULT_CONFIG_FILE,
                      help="Configuration file.")
  parser.add_argument("-V", "--verbose",
                      default=False,
                      action="store_true",
                      help="Be verbose")
  parser.add_argument("-L","--limit",
                      default=10,
                      type=int,
                      help="Number of rows to return for queries (10)")
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