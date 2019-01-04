'''
Constants methods, etc common across metrics system
'''
import logging
import dateparser
from pytz import timezone

#Default location of configuration file
DEFAULT_CONFIG_FILE="/etc/dataone/metrics/database.ini"


def textToDateTime(txt, default_tz='UTC'):
  '''
  Convert plain text to a timezone aware datetime instance.

  e.g.: "now", "yesterday", "1 year ago", "10 days from now", "2:30pm"
  Args:
    txt: Textual representation of a dateTime
    default_tz: Timezone to use when it can't be figured out.

  Returns:
    Timezone aware instance of DateTime
  '''
  logger = logging.getLogger('common')
  d = dateparser.parse(txt, settings={'RETURN_AS_TIMEZONE_AWARE': True})
  if d is None:
    logger.error("Unable to convert '%s' to a date time.", txt)
    return d
  if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
    logger.warning('No timezone information specified, assuming UTC')
    return d.replace(tzinfo = timezone('UTC'))
  return d
