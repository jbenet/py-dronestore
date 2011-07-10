



class Datastore(object):
  '''A Datastore represents storage for serialized dronestore versions.
  Datastores are general enough to be backed by all kinds of different storage:
  in-memory caches, databases, a remote cache, flat files on disk, etc.

  The general idea is to wrap a more complicated storage facility in a simple,
  uniform interface, keeping the freedom of using the right tools for the job.
  In particular, a Datastore can aggregate other datastores in interesting ways,
  like sharded (to distribute load) or tiered access (caches before databases).

  While Datastores should be written general enough to accept all sorts of
  values, some implementations will undoubtedly have to be specific (e.g. SQL
  databases where fields should be decomposed into columns), particularly those
  that support Queries.

  This interface matches the Drone's well, as it supports each of its calls.
  '''

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

  def query(self, query):
    '''Returns a sequence of objects matching criteria expressed in `query`'''
    raise NotImplementedError



class DictDatastore(Datastore):
  '''Simple straw-man in-memory datastore backed by a dict.

  WARNING: it does not evict entries so it will grow indefinitely. use this for
    testing, short-lived, or small working-set programs.
  '''

  def __init__(self):
    self._items = {}

  def get(self, key):
    '''Return the object named by key.'''
    try:
      return self._items[key]
    except KeyError, e:
      return None

  def put(self, key, value):
    '''Stores the object.'''
    if value is None:
      self.delete(key)
    else:
      self._items[key] = value

  def delete(self, key):
    '''Removes the object.'''
    try:
      del self._items[key]
    except KeyError, e:
      pass

  def contains(self, key):
    '''Returns whether the object is in this datastore.'''
    return key in self._items

  def query(self, query):
    '''Returns a sequence of objects matching criteria expressed in `query`'''
    # entire dataset already in memory, so ok to apply query naively
    return query(self._items.values())

  def __len__(self):
    return len(self._items)



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

  def query(self, query):
    '''Returns a sequence of objects matching criteria expressed in `query`'''
    # queries hit the last (most complete) datastore
    return self._stores[-1].query(query)




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

  def query(self, query):
    '''Returns a sequence of objects matching criteria expressed in `query`'''
    items = []
    results = [s.query(query) for s in self._stores]
    map(items.extend, results)
    items = sorted(items, cmp=query.orderFn)
    return items[:query.limit]



