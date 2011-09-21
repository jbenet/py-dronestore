
import basic
import pymongo
import bson

__version__ = '1'

kKEY = 'key'
kVAL = 'val'
kMONGOID = '_id'
kWRAPPED = 'dswrapped'

class MongoDatastore(basic.Datastore):
  '''Represents a Mongo database as a datastore.'''

  def __init__(self, mongoDatabase):
    self.database = mongoDatabase
    self._indexed = {}


  def _collectionForType(self, type):
    '''Returns the `collection` corresponding to `type`.'''

    # place objects in collections based on the keyType
    collection = self.database[type]

    # ensure there is an index, at least once per run.
    if type not in self._indexed:
      collection.create_index(kKEY, unique=True)
      self._indexed[type] = True

    return collection

  def _collection(self, key):
    '''Returns the `collection` corresponding to `key`.'''
    return self._collectionForType(key.type())

  @staticmethod
  def _wrap(key, value):
    '''Returns a value to insert. Non-documents are wrapped in a document.'''
    if not isinstance(value, dict) or kKEY not in value or value[kKEY] != key:
      return { kKEY:key, kVAL:value, kWRAPPED:True}

    if kMONGOID in value:
      del value[kMONGOID]

    return value

  @staticmethod
  def _unwrap(value):
    '''Returns a value to return. Wrapped-documents are unwrapped.'''
    if value is not None and kWRAPPED in value and value[kWRAPPED]:
      return value[kVAL]

    if isinstance(value, dict) and kMONGOID in value:
      del value[kMONGOID]

    return value


  def get(self, key):
    '''Return the object named by key.'''
    # query the corresponding mongodb collection for this key
    value = self._collection(key).find_one( { kKEY:str(key) } )
    return self._unwrap(value)

  def put(self, key, value):
    '''Stores the object.'''
    sKey = str(key)
    value = self._wrap(sKey, value)

    # update (or insert) the relevant document matching key
    self._collection(key).update( { kKEY:sKey }, value, upsert=True, safe=True)

  def delete(self, key):
    '''Removes the object.'''
    self._collection(key).remove( { kKEY:str(key) } )

  def contains(self, key):
    '''Returns whether the object is in this datastore.'''
    return self._collection(key).find( { kKEY:str(key) } ).count() > 0

  def query(self, query):
    '''Returns a sequence of objects matching criteria expressed in `query`'''
    coll = self._collectionForType(query.type)
    return QueryTranslate.collectionQuery(coll, query)


class UnwrapperCursor(object):
  '''An iterator object to wrap around the mongodb cursor.
  Ensures objects fetched by queries are clear of any wrapping
  '''

  def __init__(self, cursor):
    self.cursor = cursor

  def __iter__(self):
    return self

  def next(self):
    return MongoDatastore._unwrap(self.cursor.next())



class QueryTranslate(object):
  '''Translates queries from dronestore queries to mongodb queries.'''
  COND_OPS = { '>':'$gt', '>=':'$gte', '!=':'$ne', '<=':'$lte', '<':'$lt' }
  VERSION_FIELDS = \
    ['key', 'hash', 'parent', 'created', 'committed', 'attributes', 'type']

  @classmethod
  def collectionQuery(self, collection, query):
    cursor = collection.find(self.filters(query.filters))
    if len(query.orders) > 0:
      cursor.sort(self.orders(query.orders))
    if query.offset > 0:
      cursor.skip(query.offset)
    cursor.limit(query.limit)
    return UnwrapperCursor(cursor)

  @classmethod
  def field(cls, field):
    if field in cls.VERSION_FIELDS:
      return field
    return 'attributes.%s.value' % field

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
    keys = [cls.field(o.field) for o in orders]
    vals = [1 if o.isAscending() else -1 for o in orders]
    return zip(keys, vals)


