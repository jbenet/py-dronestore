
from model import Key, Version, Model
from datastore import Datastore

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
  Each drone consists of a datastore (or set of datastores) and an id.
  '''

  def __init__(self, droneid, store):
    '''Initializes drone with given id and datastore.'''
    if not isinstance(droneid, Key):
      droneid = Key(droneid)
    if not isinstance(store, Datastore):
      raise ValueError('store must be an instance of %s' % Datastore)

    self._droneid = droneid
    self._store = store

  @property
  def droneid(self):
    '''This drone's identifier.'''
    return self._droneid

  def put(self, entity):
    '''Stores `entity` in the datastore.'''
    if not isinstance(entity, Model):
      raise TypeError('entity is not of type %s' % Model)

    if entity.isDirty():
      raise ValueError('cannot store a dirty (uncommitted) entity.')

    self._store.put(entity.key, entity)

  def get(self, key):
    '''Retrieves the current entity addressed by `key`'''
    if not isinstance(key, Key):
      raise ValueError('key must be of type %s' % Key)

    return self._store.get(key)

  def merge(self, newVersionOrEntity):
    '''Merges a new version of an instance with the current one in the store.'''

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
    instance = self._store.get(key) #THINKME(jbenet): try contains first?
    if instance is None:
      raise KeyError('no object with key %s' % key)
    elif not isinstance(instance, Model):
      raise KeyError('no object with key %s (found %s)' % (key, instance))

    # merge changes
    merge.merge(instance, new_version)

    # store it back
    self._store.put(key, instance)

  def delete(self, key):
    '''Deletes the entity addressed by `key` from the datastore.'''
    if not isinstance(key, Key):
      raise ValueError('key must be of type %s' % Key)

    self._store.delete(key)


