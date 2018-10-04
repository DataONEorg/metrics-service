import sys
import logging
import argparse

from d1_logagg import recordprocessor
from d1_logagg import sessionid

SESSION_DURATION=60.0

def computeSessions(output_file):
  record_processor = recordprocessor.RecordProcessor(output_file = output_file)
  historical = record_processor.loadHistory(max_duration=SESSION_DURATION)
  logging.debug("%d records in history", len(historical))
  logging.debug("earliest = %s", historical[0]["dateLogged"])
  logging.debug("latest = %s", historical[-1]["dateLogged"])


def main():
  parser = argparse.ArgumentParser(description=__doc__,
    formatter_class=argparse.RawDescriptionHelpFormatter)
  parser.add_argument('-l', '--log_level',
                      action='count',
                      default=0,
                      help='Set logging level, multiples for more detailed.')
  parser.add_argument("-Y","--dryrun",
                      action="store_true",
                      help="Dry run - don't make any changes.")
  parser.add_argument("-d", "--dest",
                      default=recordprocessor.RecordProcessor.DEFAULT_LOG_FILE,
                      help="Name of destination log file")
  args = parser.parse_args()
  # Setup logging verbosity
  levels = [logging.WARNING, logging.INFO, logging.DEBUG]
  level = levels[min(len(levels) - 1, args.log_level)]
  logging.basicConfig(level=level,
                      format="%(asctime)s %(name)s %(levelname)s: %(message)s")
  computeSessions(args.dest)


if __name__ == "__main__":
  sys.exit(main())


