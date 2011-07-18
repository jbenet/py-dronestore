
# import json. should ass py-yajl here
try:
  import simplejson as json
except:
  try:
    import cjson as json
  except:
    import json

import bson

import nanotime
import datetime

def clean(value):
  '''Cleans up a value for insertion into a SerialRepresentation'''

  if isinstance(value, dict):
    value = dict([(clean(k), clean(v)) for k, v in value.items()])
  elif isinstance(value, list) or isinstance(value, tuple):
    value = [clean(v) for v in value]
  elif isinstance(value, int) or isinstance(value, long):
    pass
  elif isinstance(value, float):
    pass
  elif isinstance(value, str):
    pass
  elif isinstance(value, unicode):
    pass
  elif isinstance(value, bool):
    pass
  elif isinstance(value, nanotime.nanotime):
    value = value.nanoseconds()
  elif isinstance(value, datetime.datetime):
    pass
  elif value is None:
    pass
  else: # catch all... turn it into a string!
    value = str(value)
  return value


class SerialRepresentation(object):

  def __init__(self, data=None):
    # internal representation is a dict.
    # consider moving to bson document object (once this is made proper)
    self._data = clean(data) if data else {}
    self._dirty = True
    self._json = None
    self._bson = None

  #-------------------------------------

  def __getitem__(self, key):
    return self._data[key]

  def __setitem__(self, key, data):
    key = clean(key)
    data = clean(data)
    if not self._dirty and self._data[key] == data:
      return # idempotent

    self._dirty = True
    self._data[key] = data

  def __delitem__(self, key):
    self._dirty = True
    del self._data[key]

  def __iter__(self):
    return iter(self._data)

  def __len__(self):
    return len(self._data)

  def __reversed__(self):
    return reversed(self._data)

  def __contains__(self, key):
    return key in self._data

  def __cmp__(self, other):
    return cmp(self._data, other._data)

  #-------------------------------------

  def __str__(self):
    return str(self.json())

  #-------------------------------------

  def data(self):
    return self._data

  def json(self):
    if not self._json or self._dirty:
      self._json = json.dumps(self._data)
    return self._json

  def bson(self):
    if not self._bson or self._dirty:
      self._bson = bson.dumps(self._data)
    return self._bson

  #-------------------------------------

  @classmethod
  def from_json(cls, json_data):
    return SerialRepresentation(json.loads(json_data))

  @classmethod
  def from_bson(cls, bson_data):
    return SerialRepresentation(bson.loads(bson_data))

  #-------------------------------------

