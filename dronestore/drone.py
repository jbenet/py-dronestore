
from model import Key, Version, Model
from query import Query, InstanceIterator
from datastore import Datastore, DictDatastore
from .util.serial import SerialRepresentation

class Drone(object):
  '''Drone represents the logical unit of storage in dronestore.
  Each drone consists of a datastore (or set of datastores) and an id.
  '''

  #FIXME(jbenet): remove DictDatastore as a default?
  def __init__(self, droneid, store=DictDatastore()):
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

  def __str__(self):
    return '<dronestore.drone.Drone object at %s %s>' % (id(self), self.droneid)

  @classmethod
  def _cleanVersion(cls, parameter):
    '''Extracts the version from input.'''
    if isinstance(parameter, Version):
      return parameter
    elif isinstance(parameter, Model):
      if parameter.isDirty():
        raise ValueError('cannot store entities with uncommitted changes')
      return parameter.version

    raise TypeError('expected input of type %s or %s' % (Version, Model))


  def put(self, versionOrEntity):
    '''Stores the current version of `entity` in the datastore.'''
    version = self._cleanVersion(versionOrEntity)
    self._store.put(version.key, version.serialRepresentation.data())
    return versionOrEntity


  def get(self, key):
    '''Retrieves the current entity addressed by `key`'''
    if not isinstance(key, Key):
      raise ValueError('key must be of type %s' % Key)

    # lookup the key in the datastore
    data = self._store.get(key)
    if data is None:
      return data

    # handle the data. if any conversion fails, propagate the exception up.
    serialRep = SerialRepresentation(data)
    version = Version(serialRep)
    return Model.from_version(version)


  def merge(self, newVersionOrEntity):
    '''Merges a new version of an instance with the current one in the store.'''

    # get the new version
    new_version = self._cleanVersion(newVersionOrEntity)

    # get the instance
    key = new_version.key
    curr_instance = self.get(key) #THINKME(jbenet): try contains first?

    # brand new version. just store it.
    if curr_instance is None:
      self.put(new_version)
      return Model.from_version(new_version)

    # NOTE: semantically, we must merge into the current instance in the drone
    # so that merge strategies favor the incumbent version.
    curr_instance.merge(new_version)

    # store it back
    self.put(curr_instance)
    return curr_instance


  def contains(self, key):
    '''Returns whether the datastore contains the entity addressed by `key`.'''
    if not isinstance(key, Key):
      raise ValueError('key must be of type %s' % Key)

    return self._store.contains(key)


  def delete(self, key):
    '''Deletes the entity addressed by `key` from the datastore.'''
    if not isinstance(key, Key):
      raise ValueError('key must be of type %s' % Key)

    self._store.delete(key)

  def query(self, query):
    '''Queries the datastore for objects matching `query`.'''
    return InstanceIterator(self._store.query(query))


