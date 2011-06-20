



class Datastore(object):
  '''Interface for Datastore Objects.'''

  def get(self, key):
    '''Return the object named by key.'''
    raise NotImplementedError

  def put(self, key, value):
    '''Stores the object.'''
    raise NotImplementedError

  def delete(self, key):
    '''Removes the object.'''
    raise NotImplementedError

  def contains(self, key):
    '''Returns whether the object is in this datastore.'''
    raise NotImplementedError








class DatastoreCollection(Datastore):
  '''Represents a collection of datastores.'''

  def __init__(self, stores=[]):
    '''Initialize the datastore with any provided datastores.'''
    if not isinstance(stores, list):
      stores = list(stores)

    for store in stores:
      if not isinstance(store, Datastore):
        raise TypeError("all stores must be of type %s" % Datastore)

    self._stores = stores

  def datastore(self, index):
    return self._stores[index]

  def appendDatastore(self, store):
    if not isinstance(store, Datastore):
      raise TypeError("stores must be of type %s" % Datastore)

    self._stores.append(store)

  def removeDatastore(self, store):
    self._stores.remove(store)

  def insertDatastore(self, index, store):
    if not isinstance(store, Datastore):
      raise TypeError("stores must be of type %s" % Datastore)

    self._stores.insert(index, store)





class TieredDatastore(DatastoreCollection):
  '''Represents a hierarchical collection of datastores.
  Each datastore is queried in order. This is helpful to organize access
  in terms of speed (i.e. hit caches first).
  '''

  def get(self, key):
    '''Return the object named by key.'''
    value = None
    for store in self._stores:
      value = store.get(key)
      if value is not None:
        break

    # add model to lower stores only
    if value is not None:
      for store2 in self._stores:
        if store == store2:
          break
        store2.put(key, value)

    return value

  def put(self, key, value):
    '''Stores the object in all stores.'''
    for store in self._stores:
      store.put(key, value)

  def delete(self, key):
    '''Removes the object from all stores.'''
    for store in self._stores:
      store.delete(key)

  def contains(self, key):
    '''Returns whether the object is in this datastore.'''
    for store in self._stores:
      if store.contains(key):
        return True

    return False





class ShardedDatastore(DatastoreCollection):
  '''Represents a collection of datastore shards.
  A datastore is selected based on a sharding function.

  sharding functions should take a Key and return an integer.

  WARNING: adding or removing datastores while running may severely affect
           consistency. Also ensure the order is correct upon initialization.
           While this is not as important for caches, it is crucial for
           persistent atastore.
  '''

  def __init__(self, stores=[], shardingfn=hash):
    '''Initialize the datastore with any provided datastore.'''
    if not callable(shardingfn):
      raise TypeError('shardingfn (type %s) is not callable' % type(shardingfn))

    super(ShardedDatastore, self).__init__(stores)
    self._shardingfn = shardingfn


  def shard(self, key):
    return self._shardingfn(key) % len(self._stores)

  def shardDatastore(self, key):
    return self.datastore(self.shard(key))


  def get(self, key):
    '''Return the object named by key from the corresponding datastore.'''
    return self.shardDatastore(key).get(key)

  def put(self, key, value):
    '''Stores the object to the corresponding datastore.'''
    self.shardDatastore(key).put(key, value)

  def delete(self, key):
    '''Removes the object from the corresponding datastore.'''
    self.shardDatastore(key).delete(key)

  def contains(self, key):
    '''Returns whether the object is in this datastore.'''
    return self.shardDatastore(key).contains(key)




