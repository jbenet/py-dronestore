


class Database(object):
  '''Interface for Database Objects.'''

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
    '''Returns whether the object is in this database.'''
    raise NotImplementedError








class DatabaseCollection(Database):
  '''Represents a collection of databases.'''

  def __init__(self, dbs=[]):
    '''Initialize the database with any provided databases.'''
    if not isinstance(dbs, list):
      dbs = list(dbs)

    for db in dbs:
      if not isinstance(db, Database):
        raise TypeError("dbs must be of type Database")

    self._dbs = dbs

  def database(self, index):
    self._dbs[index]

  def appendDatabase(self, db):
    if not isinstance(db, Database):
      raise TypeError("dbs must be of type Database")

    self._dbs.append(db)

  def removeDatabase(self, db):
    self._dbs.remove(db)

  def insertDatabase(self, index, db):
    if not isinstance(db, Database):
      raise TypeError("dbs must be of type Database")

    self._dbs.insert(index, db)





class TieredDatabase(DatabaseCollection):
  '''Represents a hierarchical collection of databases.
  Each database is queried in order. This is helpful to organize access
  in terms of speed (i.e. hit caches first).
  '''

  def get(self, key):
    '''Return the object named by key.'''
    model = None
    for db in self._dbs:
      model = db.get(key)
      if model is not None:
        break

    # add model to lower dbs
    if model is not None:
      for db2 in self._dbs:
        if db == db2:
          break
        db2.put(key, model)

    return model

  def put(self, key, value):
    '''Stores the object in all dbs.'''
    for db in self._dbs:
      db.put(key, value)

  def delete(self, key):
    '''Removes the object from all dbs.'''
    for db in self._dbs:
      db.delete(key)

  def contains(self, key):
    '''Returns whether the object is in this database.'''
    for db in self._dbs:
      if db.contains(key):
        return True

    return False





class ShardedDatabase(DatabaseCollection):
  '''Represents a collection of database shards.
  A database is selected based on a sharding function.

  sharding functions should take a Key and return an integer.

  WARNING: adding or removing databases while running may severely affect
           consistency. Also ensure the order is correct upon initialization.
           While this is not as important for caches, it is crucial for
           persistent databases.
  '''

  def __init__(self, dbs=[], shardingfn=hash):
    '''Initialize the database with any provided database.'''
    if not callable(shardingfn):
      raise TypeError('shardingfn (type %s) is not callable' % type(shardingfn))

    super(ShardedDatabase, self).__init__(dbs)
    self._shardingfn = shardingfn


  def shard(self, key):
    return self._shardingfn(key) % len(self._dbs)

  def shardDatabase(self, key):
    return self._dbs[self.shard(key)]


  def get(self, key):
    '''Return the object named by key from the corresponding database.'''
    return self.shardDatabase(key).get(key)

  def put(self, key, value):
    '''Stores the object to the corresponding database.'''
    self.shardDatabase(key).put(key, value)

  def delete(self, key):
    '''Removes the object from the corresponding database.'''
    self.shardDatabase(key).delete(key)

  def contains(self, key):
    '''Returns whether the object is in this database.'''
    return self.shardDatabase(key).contains(key)




