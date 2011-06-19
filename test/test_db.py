
import unittest

from dronestore import db
from dronestore.model import Key, Model

from test_model import TestPerson

class TestDatabase(db.Database):

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
    '''Returns whether the object is in this database.'''
    return key in self._items

  def __len__(self):
    return len(self._items)



class TestDB(unittest.TestCase):


  def test_simple(self, dbs=[]):

    def checkLength(len):
      try:
        for dbn in dbs:
          self.assertEqual(len(dbn), 1000)
      except TypeError, e:
        pass

    if len(dbs) == 0:
      db1 = TestDatabase()
      db2 = TestDatabase()
      db3 = TestDatabase()
      dbs = [db1, db2, db3]

    pkey = Key('/dfadasfdsafdas/')

    checkLength(0)

    # insert 1000 elems
    for value in range(0, 1000):
      key = pkey.child(value)
      for dbn in dbs:
        self.assertFalse(dbn.contains(key))
        dbn.put(key, value)
        self.assertTrue(dbn.contains(key))
        self.assertEqual(dbn.get(key), value)

    # reassure they're all there.
    checkLength(1000)

    for value in range(0, 1000):
      key = pkey.child(value)
      for dbn in dbs:
        self.assertTrue(dbn.contains(key))
        self.assertEqual(dbn.get(key), value)

    checkLength(1000)

    # change 1000 elems
    for value in range(0, 1000):
      key = pkey.child(value)
      for dbn in dbs:
        self.assertTrue(dbn.contains(key))
        dbn.put(key, value + 1)
        self.assertTrue(dbn.contains(key))
        self.assertNotEqual(value, dbn.get(key))
        self.assertEqual(value + 1, dbn.get(key))

    checkLength(1000)

    # remove 1000 elems
    for value in range(0, 1000):
      key = pkey.child(value)
      for dbn in dbs:
        self.assertTrue(dbn.contains(key))
        dbn.delete(key)
        self.assertFalse(dbn.contains(key))

    checkLength(0)

  def test_tiered(self):

    db1 = TestDatabase()
    db2 = TestDatabase()
    db3 = TestDatabase()
    tdb = db.TieredDatabase([db1, db2, db3])

    k1 = Key('1')
    k2 = Key('2')
    k3 = Key('3')

    db1.put(k1, '1')
    db2.put(k2, '2')
    db3.put(k3, '3')

    self.assertTrue(db1.contains(k1))
    self.assertFalse(db2.contains(k1))
    self.assertFalse(db3.contains(k1))
    self.assertTrue(tdb.contains(k1))

    self.assertEqual(tdb.get(k1), '1')
    self.assertEqual(db1.get(k1), '1')
    self.assertFalse(db2.contains(k1))
    self.assertFalse(db3.contains(k1))

    self.assertFalse(db1.contains(k2))
    self.assertTrue(db2.contains(k2))
    self.assertFalse(db3.contains(k2))
    self.assertTrue(tdb.contains(k2))

    self.assertEqual(db2.get(k2), '2')
    self.assertFalse(db1.contains(k2))
    self.assertFalse(db3.contains(k2))

    self.assertEqual(tdb.get(k2), '2')
    self.assertEqual(db1.get(k2), '2')
    self.assertEqual(db2.get(k2), '2')
    self.assertFalse(db3.contains(k2))

    self.assertFalse(db1.contains(k3))
    self.assertFalse(db2.contains(k3))
    self.assertTrue(db3.contains(k3))
    self.assertTrue(tdb.contains(k3))

    self.assertEqual(db3.get(k3), '3')
    self.assertFalse(db1.contains(k3))
    self.assertFalse(db2.contains(k3))

    self.assertEqual(tdb.get(k3), '3')
    self.assertEqual(db1.get(k3), '3')
    self.assertEqual(db2.get(k3), '3')
    self.assertEqual(db3.get(k3), '3')

    tdb.delete(k1)
    tdb.delete(k2)
    tdb.delete(k3)

    self.assertFalse(tdb.contains(k1))
    self.assertFalse(tdb.contains(k2))
    self.assertFalse(tdb.contains(k3))

    self.test_simple([tdb])

  def test_sharded(self):

    db1 = TestDatabase()
    db2 = TestDatabase()
    db3 = TestDatabase()
    db4 = TestDatabase()
    db5 = TestDatabase()
    dbs = [db1, db2, db3, db4, db5]
    sdb = db.ShardedDatabase(dbs)
    sumlens = lambda dbs: sum(map(lambda db: len(db), dbs))

    def checkFor(key, value, sdb, shard=None):
      correct_shard = sdb._dbs[hash(key) % len(sdb._dbs)]

      for db in sdb._dbs:
        if shard and db == shard:
          self.assertTrue(db.contains(key))
          self.assertEqual(db.get(key), value)
        else:
          self.assertFalse(db.contains(key))

      if correct_shard == shard:
        self.assertTrue(sdb.contains(key))
        self.assertEqual(sdb.get(key), value)
      else:
        self.assertFalse(sdb.contains(key))

    self.assertEqual(sumlens(dbs), 0)
    # test all correct.
    for value in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = dbs[hash(key) % len(dbs)]
      checkFor(key, value, sdb)
      shard.put(key, value)
      checkFor(key, value, sdb, shard)
    self.assertEqual(sumlens(dbs), 1000)

    # ensure its in the same spots.
    for i in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = dbs[hash(key) % len(dbs)]
      checkFor(key, value, sdb, shard)
      shard.put(key, value)
      checkFor(key, value, sdb, shard)
    self.assertEqual(sumlens(dbs), 1000)

    # ensure its in the same spots.
    for value in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = dbs[hash(key) % len(dbs)]
      checkFor(key, value, sdb, shard)
      sdb.put(key, value)
      checkFor(key, value, sdb, shard)
    self.assertEqual(sumlens(dbs), 1000)

    # ensure its in the same spots.
    for value in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      shard = dbs[hash(key) % len(dbs)]
      checkFor(key, value, sdb, shard)
      if value % 2 == 0:
        shard.delete(key)
      else:
        sdb.delete(key)
      checkFor(key, value, sdb)
    self.assertEqual(sumlens(dbs), 0)

    # try out adding it to the wrong shards.
    for value in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = dbs[(hash(key) + 1) % len(dbs)]
      checkFor(key, value, sdb)
      incorrect_shard.put(key, value)
      checkFor(key, value, sdb, incorrect_shard)
    self.assertEqual(sumlens(dbs), 1000)

    # ensure its in the same spots.
    for value in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = dbs[(hash(key) + 1) % len(dbs)]
      checkFor(key, value, sdb, incorrect_shard)
      incorrect_shard.put(key, value)
      checkFor(key, value, sdb, incorrect_shard)
    self.assertEqual(sumlens(dbs), 1000)

    # this wont do anything
    for value in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = dbs[(hash(key) + 1) % len(dbs)]
      checkFor(key, value, sdb, incorrect_shard)
      sdb.delete(key)
      checkFor(key, value, sdb, incorrect_shard)
    self.assertEqual(sumlens(dbs), 1000)

    # this will place it correctly.
    for value in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      incorrect_shard = dbs[(hash(key) + 1) % len(dbs)]
      correct_shard = dbs[(hash(key)) % len(dbs)]
      checkFor(key, value, sdb, incorrect_shard)
      sdb.put(key, value)
      incorrect_shard.delete(key)
      checkFor(key, value, sdb, correct_shard)
    self.assertEqual(sumlens(dbs), 1000)

    # this will place it correctly.
    for value in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % value)
      correct_shard = dbs[(hash(key)) % len(dbs)]
      checkFor(key, value, sdb, correct_shard)
      sdb.delete(key)
      checkFor(key, value, sdb)
    self.assertEqual(sumlens(dbs), 0)

    self.test_simple([sdb])


  def test_lru(self):

    from dronestore.db import lrucache

    lru1 = lrucache.LRUCache(1000)
    lru2 = lrucache.LRUCache(2000)
    lru3 = lrucache.LRUCache(3000)
    lrus = [lru1, lru2, lru3]

    for value in range(0, 3000):
      key = Key('/LRU/%d' % value)
      for lru in lrus:
        self.assertFalse(lru.contains(key))

        lru.put(key, value)

        self.assertTrue(lru.contains(key))
        self.assertEqual(lru.get(key), value)

    self.assertEqual(len(lru1), 1000)
    self.assertEqual(len(lru2), 2000)
    self.assertEqual(len(lru3), 3000)

    for value in range(0, 3000):
      key = Key('/LRU/%d' % value)
      self.assertEqual(lru1.contains(key), value >= 2000)
      self.assertEqual(lru2.contains(key), value >= 1000)
      self.assertTrue(lru3.contains(key))

    lru1.clear()
    lru2.clear()
    lru3.clear()

    self.assertEqual(len(lru1), 0)
    self.assertEqual(len(lru2), 0)
    self.assertEqual(len(lru3), 0)

    self.test_simple(lrus)


  def test_mongo(self):

    import os
    import sys
    sys.path.append('/Users/jbenet/git/mongo/pymongo')

    from dronestore.db import mongo
    import pymongo

    conn = pymongo.Connection()
    mdb = mongo.MongoDatabase(conn.testdb.testcollection)

    p1 = TestPerson('A')
    p2 = TestPerson('B')
    p3 = TestPerson('C')

    p1.first = 'A'
    p2.first = 'B'
    p3.first = 'C'

    p1.last = 'A'
    p2.last = 'B'
    p3.last = 'C'

    p1.commit()
    p2.commit()
    p3.commit()

    self.assertFalse(mdb.contains(p1.key))
    self.assertFalse(mdb.contains(p2.key))
    self.assertFalse(mdb.contains(p3.key))

    mdb.put(p1.key, p1)
    mdb.put(p2.key, p2)
    mdb.put(p3.key, p3)

    self.assertTrue(mdb.contains(p1.key))
    self.assertTrue(mdb.contains(p2.key))
    self.assertTrue(mdb.contains(p3.key))
    self.assertEqual(p1, mdb.get(p1.key))
    self.assertEqual(p2, mdb.get(p2.key))
    self.assertEqual(p3, mdb.get(p3.key))

    mdb.delete(p1.key)
    mdb.delete(p2.key)
    mdb.delete(p3.key)

    self.assertFalse(mdb.contains(p1.key))
    self.assertFalse(mdb.contains(p2.key))
    self.assertFalse(mdb.contains(p3.key))


