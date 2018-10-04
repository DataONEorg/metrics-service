import re
import logging
from d1_logagg import sessionid

class SessionStates(object):

  EVENT_TYPES = ["any", "read", ]
  IPADDRESS = "ipAddress"
  DATE_LOGGED = "dateLogged"
  IP_PATTERN = re.compile("^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")

  def __init__(self, duration, event_type, id_generator):
    self._duration = duration
    self._event_type = event_type.lower().trim()
    assert self._event_type in SessionStates.EVENT_TYPES
    self._session_name = "sess_{}{}".format( self._event_type[0], self._duration)
    self._sessions = {} #keyed by IP address, entry = {"id": id, "start_time": st, "last_time"=lt}
    self._id_generator = id_generator


  def dt(self, a, b):
    '''
    Compute a-b in minutes
    Args:
      a: most recent time in datetime
      b: oldest time in datetime

    Returns:
      Difference in decimal minutes
    '''
    return (a-b).total_seconds / 60.0


  def isValidIpAddress(self, ip):
    '''
    A valid address is one that looks like an IP address and is not localhost.

    Args:
      ip: ip address to test

    Returns:
      Boolean
    '''
    ip = ip.strip()
    test = SessionStates.IP_PATTERN.match(ip)
    if test:
      if ip != "127.0.0.1":
        return True
    return False


  def addRecord(self, record, allow_create=True):
    '''
    Adds information to an existing session or creates a new one
    Args:
      record: A log record
      allow_create: Allow creation of new sessionIds

    Returns:
      (SessionID, is_new)
    '''
    ip = record.get( SessionStates.IPADDRESS )
    id = None
    if self.isValidIpAddress(ip):
      tstamp = record.get( SessionStates.DATE_LOGGED )
      if tstamp is not None:
        existing = self._sessions.get( ip )
        is_new = False
        if existing is None:
          is_new = True
        else:
          delta = self.dt(tstamp, existing.get( "start_time" ))
          if delta > self._duration:
            is_new = True
        if is_new and allow_create:
          session = {"id": self._newSessionId(),
                     "start_time": tstamp,
                     "last_time": tstamp
                     }
          self._sessions[ ip ] = session
          return ( session["id"], True, )
        existing["last_time"] = tstamp
        self._sessions[ip] = existing
        return (existing["id"], False, )
    return (None, False, )


  def expireSessions(self):
    '''
    Based on the most recent time stamp in the sessions, expire those that
    are older than the duration for this set of session states.

    Returns:
      Total number of sessions
    '''
    newest = None
    for ip in self._sessions:
      session = self._sessions[ip]
      if newest is None:
        newest = session["last_time"]
      else:
        if session["last_time"] > newest:
          newest = session["last_time"]
    for ip in self._sessions:
      delta = self.dt(newest, session["start_time"])
      if delta > self._duration:
        self._sessions.delete(ip)
    return len(self._sessions)


  def initializeSessionStates(self, records):
    '''
    Given a list of already computed records, initialize session state

    Args:
      records: list of log records with already computed session state

    Returns:
      number of active sessions
    '''
    self._sessions = {}
    for record in records:
      if record.get( self._session_name ) is not None:
        self.add_session( record, allow_create=False )
    return self.expireSessions()


  def _newSessionId(self):
    '''
    Generate a new session ID

    A sessionID is a positive integer that is unique to a specific session.

    Returns: sessionId
    '''
    new_id = self._id_generator.getNewId()
    return new_id


  def getSessionInformation(self, record):
    '''
    Returns either a new or existing session ID.
    Args:
      record: event record to be evaluated

    Returns:
      (session_name, ID, is_new) where is_new is boolean True if a new session generated
    '''
    info = self.addRecord( record, allow_create=True)
    return ( self._session_name, info[0], info[1])

