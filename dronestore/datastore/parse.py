
import basic
import json
import requests

from ..attribute import DateTimeAttribute


__version__ = '1'


class ParseException(Exception):
  pass


class ParseConnection(object):
  '''Represents a connection via the Parse API.

  Every request is a new HTTP request.

  '''

  API_URL = 'https://api.parse.com/1/classes'

  json_header = {'content-type' : 'application/json'}

  def __init__(self, appid, masterkey):
    self.appid = appid
    self.masterkey = masterkey

  @property
  def auth(self):
    '''Return the auth tuple for this connection'''
    return (self.appid, self.masterkey)

  def request(self, method, className, objectId='', data=None):
    '''Make a request'''
    assert method in ['GET', 'POST', 'PUT', 'DELETE']

    url = '%s/%s' % (self.API_URL, className)
    if objectId:
      url += '/%s' % str(objectId)

    kwargs = {}
    kwargs['auth'] = self.auth
    if data:
      kwargs['data'] = data
      if isinstance(data, str):
        kwargs['headers'] = self.json_header

    res = requests.request(method, url, **kwargs)
    if res.status_code not in [200, 201]:
      errstr = '%s %s (%s) failed with status code %s'
      raise ParseException(errstr % (method, url, kwargs, res.status_code))

    return res


  def get(self, className, objectId):
    '''Retrieve Parse object with id `objectId` and class `className`'''
    res = self.request('GET', className, objectId=objectId)
    if res.status_code is not 200:
      raise ParseException(('GET', className, objectId, res))
    return json.loads(res.content)

  def post(self, className, data):
    '''Post new Parse object with class `className` and data `data`'''
    res = self.request('POST', className, data=json.dumps(data))
    return json.loads(res.content)

  def put(self, className, objectId, data):
    '''Put updated `data` for object with class `className` and id `objectId`'''
    res = self.request('PUT', className, objectId=objectId, data=data)
    if res.status_code is not 200:
      raise ParseException(('PUT', className, objectId, json.dumps(data), res))
    return json.loads(res.content)

  def query(self, className, query):
    '''Query for objects with class `className` and matching `query`'''

    req = {}
    for param, value in query.items():
      assert param in ['where', 'order', 'limit', 'skip']
      req[param] = json.dumps(value)
    res = self.request('GET', className, data=req)

    if res.status_code is not 200:
      raise ParseException(('GET', className, req, res))
    return json.loads(res.content)

  def delete(self, className, objectId):
    '''Delete Parse object with id `objectId` and class `className`'''
    res = self.request('DELETE', className, objectId=objectId)
    if res.status_code is not 200:
      raise ParseException(('DELETE', className, objectId, res))
    return json.loads(res.content)





VERSION_FIELDS = \
  ['key', 'hash', 'parent', 'created', 'committed', 'attributes', 'type']


def _sanitize_parse_value(value):
  '''Return a sanitized value, without parse idiosyncrasies'''
  # dictionary
  if isinstance(value, dict):
    if u'__type' in value and value[u'__type'] == u'Date' and 'iso' in value:
      value = DateTimeAttribute._datetime_from_iso_string(value['iso'])
    else:
      value = dict([(k, _sanitize_parse_value(v)) for k, v in value.items()])

  # lists
  elif isinstance(value, list):
    value = map(_sanitize_parse_value, value)

  return value

def _version_data_from_parse_object(pobj):
  '''return the version data extracted frob `pobj`'''
  ver = {}
  for field in VERSION_FIELDS:
    ver[field] = _sanitize_parse_value(pobj[field])
  return ver


def _update_parse_obj_from_version_data(pobj, ver):
  '''return the version data extracted frob `pobj`'''

  for field in VERSION_FIELDS:
    pobj[field] = ver[field]

  # WARNING: this is a work-around because Parse does not currently (as of
  # 2011-10-25) allow querying nested documents. This *doubles* the data
  # transfer size. Fix this when Parse does support these queries.

  for attr, data in ver['attributes'].items:
    for key, value in data.items():
      compositeKey = 'attributes.%s.%s' % (attr, key)
      compositeKey = _parse_field_for_field(compositeKey)
      pobj[compositeKey] = value

  return pobj


def _parse_field_for_field(field):
  '''return the adjusted parse field for `field`'''

  if field in VERSION_FIELDS:
    return field

  if not field.startswith('attributes.'):
    field = 'attributes.%s.value' % field

  field = field.replace('.', '1')
  field = filter(str.isalnum, field)
  return field





class ParseDatastore(basic.Datastore):
  '''Represents a Parse (parse.com) database as a datastore.'''

  def __init__(self, appid, masterkey):
    self.parseconn = ParseConnection(appid, masterkey)

  def parseObjectForKey(self, key):
    '''Return the parse object for given `key`'''
    query = {'where' : {'key': str(key)}, 'limit' : 1}
    res = self.parseconn.query(key.type(), query)
    res = res['results']
    if len(res) == 0:
      return None
    return res[0]

  def get(self, key):
    '''Return the object named by key.'''
    pobj = self.parseObjectForKey(key)
    if not pobj:
      return None
    return _version_data_from_parse_object(pobj)

  def put(self, key, value):
    '''Stores the object.'''
    pobj = self.parseObjectForKey(key)
    value = _update_parse_obj_from_version_data(value)
    if pobj:
      self.parseconn.put(key.type(), pobj['objectId'], value)
    else:
      self.parseconn.post(key.type(), value)

  def delete(self, key):
    '''Removes the object.'''
    pobj = self.parseObjectForKey(key)
    if pobj:
      self.parseconn.delete(key.type(), pobj['objectId'])

  def contains(self, key):
    '''Returns whether the object is in this datastore.'''
    return self.get(key) is not None

  def query(self, query):
    '''Returns a sequence of objects matching criteria expressed in `query`'''
    pquery = QueryTranslate.query(query)
    res = self.parseconn.query(query.type, pquery)
    return UnParseIterator(res['results'])


class UnParseIterator(object):
  '''An iterator object to wrap around a parse results set'''

  def __init__(self, cursor):
    self.cursor = iter(cursor)

  def __iter__(self):
    return self

  def next(self):
    return _version_data_from_parse_object(self.cursor.next())



class QueryTranslate(object):
  '''Translates queries from dronestore queries to parse queries.'''
  COND_OPS = { '>':'$gt', '>=':'$gte', '!=':'$ne', '<=':'$lte', '<':'$lt' }

  @classmethod
  def query(self, dsquery):
    pquery = {}
    if len(dsquery.orders) > 0:
      pquery['order'] = self.orders(dsquery.orders)
    if dsquery.offset > 0:
      pquery['skip'] = dsquery.offset
    if dsquery.limit > 0:
      pquery['limit'] = dsquery.limit
    return pquery

  @classmethod
  def field(cls, field):
    return _parse_field_for_field(field)

  @classmethod
  def filter(cls, filter):
    if filter.op == '=':
      return filter.value
    return { cls.COND_OPS[filter.op] : filter.value }

  @classmethod
  def filters(cls, filters):
    keys = [cls.field(f.field) for f in filters]
    vals = [cls.filter(f) for f in filters]
    return dict(zip(keys, vals))

  @classmethod
  def orders(cls, orders):
    fds = [cls.field(o.field) for o in orders]
    ops = ['' if o.isAscending() else '-' for o in orders]
    tps = zip(fds, ops)
    return map(lambda tup: ''.join(tup), tps)


