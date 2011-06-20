
from model import Key, Version, Model

import merge


class Drone(object):

  def __init__(self, droneid, db):
    self._droneid = droneid
    self._db = db

  def droneid(self):
    '''This drone's identifier.'''
    return self._droneid

  def put(self, entity):
    '''Stores `entity` in the database.'''
    if not isinstance(entity, Model):
      raise TypeError('entity is not of type %s' % Model)
    self._dbs.put(model.key, entity)

  def get(self, key):
    '''Retrieves the current entity addressed by `key`'''
    if not isinstance(key, Key):
      key = Key(key)
    return self._dbs.get(key)

  def merge(self, newVersionOrEntity):
    '''Merges a new version of an instance with the current one in the db.'''
    if isinstance(newVersionOrEntity, Version):
      new_version = newVersionOrEntity
    elif isinstance(newVersionOrEntity, Model):
      new_version = newVersionOrEntity.version
    else:
      raise TypeError('new_version is not of type %s' % Version)

    key = new_version.key()

    instance = self._dbs.get(key) #THINKME(jbenet): try contains first?
    if instance is None:
      raise KeyError('no object with key %s' % key)
    elif not isinstance(instance, Model):
      raise KeyError('no object with key %s (found %s)' % (key, instance))

    merge.merge(instance, version)
    self._dbs.put(key, instance)

  def delete(self, key):
    '''Deletes the entity addressed by `key` from the database.'''
    if not isinstance(key, Key):
      key = Key(key)

    self._dbs.delete(key)


