
import basic
import pymongo
import bson

__version__ = '1'

kKEY = 'key'
kVAL = 'val'
kWRAPPED = 'dswrapped'

class MongoDatastore(basic.Datastore):
  '''Represents a Mongo database as a datastore.'''

  def __init__(self, mongoDatabase):
    self.database = mongoDatabase
    self._indexed = {}

  def _collection(self, key):
    '''Returns the `collection` corresponding to `key`.'''

    # place objects in collections based on the keyType
    collection = self.database[key.type()]

    # ensure there is an index, at least once per run.
    if key.type() not in self._indexed:
      collection.create_index(kKEY, unique=True)
      self._indexed[key.type()] = True

    return collection

  def get(self, key):
    '''Return the object named by key.'''

    # query the corresponding mongodb collection for this key
    value = self._collection(key).find_one( { kKEY:str(key) } )

    # check for wrapping and remove if found.
    if value is not None and kWRAPPED in value and value[kWRAPPED]:
      return value[kVAL]

    return value

  def put(self, key, value):
    '''Stores the object.'''
    sKey = str(key)

    # if given value is not suitable to insert, wrap it with a top level doc
    if not isinstance(value, dict) or kKEY not in value or value[kKEY] != sKey:
      value = { kKEY:sKey, kVAL:value, kWRAPPED:True}

    # update (or insert) the relevant document matching key
    self._collection(key).update( { kKEY:sKey }, value, upsert=True, safe=True)

  def delete(self, key):
    '''Removes the object.'''
    self._collection(key).remove( { kKEY:str(key) } )

  def contains(self, key):
    '''Returns whether the object is in this datastore.'''
    return self._collection(key).find( { kKEY:str(key) } ).count() > 0

