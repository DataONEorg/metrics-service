'''
Manage the postgres database.


'''

import argparse
import glob
import logging
import os
import sys
from pprint import pprint

from d1_metrics import common
import psycopg2
from d1_metrics import metricsdatabase


def dbcheck(args):
  #Get a logger with the current method name
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  #get metrics database instance
  database = metricsdatabase.MetricsDatabase(args.config)
  try:
    database.connect()
  except psycopg2.OperationalError as e:
    #Can't connect to database
    _L.error(e)
    print("Database not available. Does it exist? See README.rst for instructions.")
  info = database.summaryReport()
  pprint(info, indent=2)
  return 0


def dbInitialize(args):
  #Get a logger with the current method name
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  if args.sqlinit is None:
    _L.error("sqlinit glob pattern is required, e.g.: ../sql/*.sql")
    return 1
  #get metrics database instance
  database = metricsdatabase.MetricsDatabase(args.config)
  try:
    database.connect()
  except psycopg2.OperationalError as e:
    #Can't connect to database
    _L.error(e)
    print("Database not available. Does it exist? See README.rst for instructions.")
  _L.info("Loading SQL files that match: %s", args.sqlinit)
  sql_files = sorted(glob.glob(args.sqlinit))
  database.initializeDatabase(sql_files)
  _L.info("Database initialized.")
  return 0


def dbSetProperty(args):
  #Get a logger with the current method name
  _L = logging.getLogger(sys._getframe().f_code.co_name + "()")
  #get metrics database instance
  database = metricsdatabase.MetricsDatabase(args.config)
  try:
    database.connect()
  except psycopg2.OperationalError as e:
    #Can't connect to database
    _L.error(e)
    print("Database not available. Does it exist? See README.rst for instructions.")
  if args.metaval is None:
    database.deleteMetadataValue(args.metakey)
    print("Metadata property at {} deleted.".format(args.metakey))
    return 0
  database.setMetadataValue(args.metakey, args.metaval)
  v = database.getMetadataValue(args.metakey)
  print("Key {} set to {}".format(args.metakey, v))
  return 0


def main():
  commands = {
    "check":dbcheck,
    "initialize":dbInitialize,
    "setproperty": dbSetProperty,
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
  parser.add_argument("--sqlinit",
                      default=None,
                      help="Glob patttern for initialization SQL files.")
  parser.add_argument("-k", "--metakey",
                      default=None,
                      help="Key for get/set metadata property.")
  parser.add_argument("-v", "--metaval",
                      default=None,
                      help="Value for set metadata property.")
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