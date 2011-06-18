
import database
import pylru

class LRUCache(database.Database):
  '''Represents an LRU cache database. backed by pylru.'''

  def __init__(self, size):
    self._cache = pylru.lrucache(size)

  def __len__(self):
    return len(self._cache)

  def clear(self):
    self._cache.clear()

  def get(self, key):
    '''Return the object named by key.'''
    try:
      return self._cache[key]
    except KeyError, e:
      return None

  def put(self, key, value):
    '''Stores the object.'''
    self._cache[key] = value

  def delete(self, key):
    '''Removes the object.'''
    del self._cache[key]

  def contains(self, key):
    '''Returns whether the object is in this database.'''
    return key in self._cache
