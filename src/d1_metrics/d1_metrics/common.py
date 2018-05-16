'''
Constants methods, etc common across metrics system
'''
import logging
import dateparser
from pytz import timezone

DEFAULT_CONFIG_FILE="/etc/dataone/metrics/database.ini"

def textToDateTime(txt, default_tz='UTC'):
  logger = logging.getLogger('common')
  d = dateparser.parse(txt, settings={'RETURN_AS_TIMEZONE_AWARE': True})
  if d is None:
    logger.error("Unable to convert '%s' to a date time.", txt)
    return d
  if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
    logger.warning('No timezone information specified, assuming UTC')
    return d.replace(tzinfo = timezone('UTC'))
  return d
