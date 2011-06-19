
import pymongo
import database

from dronestore.model import Key, Version, Model
from dronestore.util.serial import SerialRepresentation

class MongoDatabase(database.Database):
  '''Represents an LRU cache database. backed by pylru.'''

  def __init__(self, mongoCollection):
    self.collection = mongoCollection

  def get(self, key):
    '''Return the object named by key.'''
    doc = self.collection.find_one({'key':str(key)})
    if doc is None:
      return None

    representation = SerialRepresentation(doc)
    version = Version(representation)
    entity = version.modelClass()(version)
    return entity

  def put(self, key, entity):
    '''Stores the object.'''
    representation = entity.version.serialRepresentation()
    doc = representation.data()
    self.collection.save(doc)

  def delete(self, key):
    '''Removes the object.'''
    self.collection.remove({'key':str(key)})

  def contains(self, key):
    '''Returns whether the object is in this database.'''
    return self.collection.find({'key':str(key)}).count() > 0


