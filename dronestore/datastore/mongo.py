
import basic
import pymongo
import bson

__version__ = '1'

kKEY = 'key'
kVAL = 'val'
kWRAPPED = 'dswrapped'

class MongoDatastore(basic.Datastore):
  '''Represents a Mongo database as a datastore.'''

  def __init__(self, mongoCollection):
    self.collection = mongoCollection
    self.collection.create_index(kKEY, unique=True)

  def get(self, key):
    '''Return the object named by key.'''
    value = self.collection.find_one({kKEY:str(key)})
    if value is not None and kWRAPPED in value and value[kWRAPPED]:
      return value[kVAL]
    return value

  def put(self, key, value):
    '''Stores the object.'''
    if not isinstance(value, dict) or kKEY not in value or value[kKEY] != key:
      value = {kKEY:str(key), kVAL:value, kWRAPPED:True}

    self.collection.update({kKEY:str(key)}, value, upsert=True, safe=True)

  def delete(self, key):
    '''Removes the object.'''
    self.collection.remove({kKEY:str(key)})

  def contains(self, key):
    '''Returns whether the object is in this datastore.'''
    return self.collection.find({kKEY:str(key)}).count() > 0

