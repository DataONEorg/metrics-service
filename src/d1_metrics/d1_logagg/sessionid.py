'''
Implements a class that generates new session IDs, guaranteed to be unique within a context.
'''
import os
import logging
import json
from multiprocessing import Lock

DEFAULT_CONTEXT_PATH = os.path.expanduser("~/.config/FID")
DEFAULT_CONTEXT = os.path.join( DEFAULT_CONTEXT_PATH, "state.json")
DEFAULT_ISSUER = "000"

class SessionId(object):

  def __init__(self, context=DEFAULT_CONTEXT, issuer=DEFAULT_ISSUER, allow_new=False):
    '''

    Args:
      context: path to a JSON file that records the last generated value.
    '''
    self.context = context
    if not os.path.exists(context):
      if allow_new:
        self._createContext()
      else:
        raise ValueError("Context %s not found!", context)
    self._lock = Lock()
    self._state = None


  def _createContext(self):
    os.makedirs(DEFAULT_CONTEXT_PATH,exist_ok=True)
    data = {"protocol":"FID",
            "issuer": self._issuer,
            "counter": 1,
            }
    self._write(data)


  def _read(self):
    with open(self.context, "r") as data_file:
      data = json.loads( data_file.read())
    return data


  def _write(self, data):
    with open(self.context, 'w') as data_file:
      data_file.write(json.dumps(data))


  def getNewId(self):
    '''
    Generate a new ID

    Returns:
      Integer, next unique SessionID
    '''
    lock_result = self._lock.acquire(timeout=5.0)
    if not lock_result:
      raise ValueError("Could not acquire lock for getNewId")
    data = self._read()
    data["counter"] = data["counter"] + 1
    self._write(data)
    self._lock.release()
    return "{protocol}:{issuer}:{counter}".format(**data)
