
from model import Key, Version, Model
from db import Database

import merge

#THINKME(jbenet): consider moving the interface to ONLY take versions as input
#                 and output, rather than full-fledged models.
#        Problem: hanging on to pointers to object can be
#                 dangerous as Drones can merge them or the client can clobber
#                 them.
#       Benefits: This also alows the Drone to be truly a Version repository,
#                 and store only state without having to make logical sense.
#      Drawbacks: The overhead of (de)serializing from versions every op.


class Drone(object):
  '''Drone represents the logical unit of storage in dronestore.
  Each drone consists of a database (or set of databases, see db) and an id.
  '''

  def __init__(self, droneid, db):
    '''Initializes drone with given id and database.'''
    if not isinstance(droneid, Key):
      droneid = Key(droneid)
    if not isinstance(db, Database):
      raise ValueError('db must be an instance of %s' % Database)

    self._droneid = droneid
    self._db = db

  @property
  def droneid(self):
    '''This drone's identifier.'''
    return self._droneid

  def put(self, entity):
    '''Stores `entity` in the database.'''
    if not isinstance(entity, Model):
      raise TypeError('entity is not of type %s' % Model)

    if entity.isDirty():
      raise ValueError('cannot store a dirty (uncommitted) entity.')

    self._db.put(entity.key, entity)

  def get(self, key):
    '''Retrieves the current entity addressed by `key`'''
    if not isinstance(key, Key):
      raise ValueError('key must be of type %s' % Key)

    return self._db.get(key)

  def merge(self, newVersionOrEntity):
    '''Merges a new version of an instance with the current one in the db.'''

    # get the new version
    if isinstance(newVersionOrEntity, Version):
      new_version = newVersionOrEntity
    elif isinstance(newVersionOrEntity, Model):
      if newVersionOrEntity.isDirty():
        raise ValueError('cannot merge a dirty (uncommitted) entity.')
      new_version = newVersionOrEntity.version
    else:
      raise TypeError('new_version is not of type %s' % Version, Model)

    # get the instance
    key = new_version.key()
    instance = self._db.get(key) #THINKME(jbenet): try contains first?
    if instance is None:
      raise KeyError('no object with key %s' % key)
    elif not isinstance(instance, Model):
      raise KeyError('no object with key %s (found %s)' % (key, instance))

    # merge changes
    merge.merge(instance, new_version)

    # store it back
    self._db.put(key, instance)

  def delete(self, key):
    '''Deletes the entity addressed by `key` from the database.'''
    if not isinstance(key, Key):
      raise ValueError('key must be of type %s' % Key)

    self._db.delete(key)


