
import unittest

from dronestore import db
from dronestore.model import Key

class TestDatabase(db.Database):

  def __init__(self):
    self._items = {}

  def __getitem__(self, key):
    '''Return the object named by key.'''
    return self._items[key]

  def __setitem__(self, key, value):
    '''Stores the object.'''
    self._items[key] = value

  def __delitem__(self, key):
    '''Removes the object.'''
    del self._items[key]

  def __contains__(self, key):
    '''Returns whether the object is in this database.'''
    return key in self._items

  def __len__(self):
    return len(self._items)



class TestTime(unittest.TestCase):


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
    for i in range(0, 1000):
      key = pkey.child(i)
      for dbn in dbs:
        self.assertFalse(key in dbn)
        dbn[key] = i
        self.assertTrue(key in dbn)
        self.assertEqual(i, dbn[key])

    # reassure they're all there.
    checkLength(1000)

    for i in range(0, 1000):
      key = pkey.child(i)
      for dbn in dbs:
        self.assertTrue(key in dbn)
        self.assertEqual(i, dbn[key])

    checkLength(1000)

    # change 1000 elems
    for i in range(0, 1000):
      key = pkey.child(i)
      for dbn in dbs:
        self.assertTrue(key in dbn)
        dbn[key] = i + 1
        self.assertTrue(key in dbn)
        self.assertNotEqual(i, dbn[key])
        self.assertEqual(i + 1, dbn[key])

    checkLength(1000)

    # remove 1000 elems
    for i in range(0, 1000):
      key = pkey.child(i)
      for dbn in dbs:
        self.assertTrue(key in dbn)
        del dbn[key]
        self.assertFalse(key in dbn)

    checkLength(0)

  def test_tiered(self):

    db1 = TestDatabase()
    db2 = TestDatabase()
    db3 = TestDatabase()
    tdb = db.TieredDatabase([db1, db2, db3])

    db1[Key('1')] = '1'
    db2[Key('2')] = '2'
    db3[Key('3')] = '3'

    self.assertTrue(Key('1') in db1)
    self.assertFalse(Key('1') in db2)
    self.assertFalse(Key('1') in db3)
    self.assertTrue(Key('1') in tdb)

    self.assertEqual(tdb[Key('1')], '1')
    self.assertEqual(db1[Key('1')], '1')
    self.assertFalse(Key('1') in db2)
    self.assertFalse(Key('1') in db3)

    self.assertFalse(Key('2') in db1)
    self.assertTrue(Key('2') in db2)
    self.assertFalse(Key('2') in db3)
    self.assertTrue(Key('2') in tdb)

    self.assertEqual(db2[Key('2')], '2')
    self.assertFalse(Key('2') in db1)
    self.assertFalse(Key('2') in db3)

    self.assertEqual(tdb[Key('2')], '2')
    self.assertEqual(db1[Key('2')], '2')
    self.assertEqual(db2[Key('2')], '2')
    self.assertFalse(Key('2') in db3)

    self.assertFalse(Key('3') in db1)
    self.assertFalse(Key('3') in db2)
    self.assertTrue(Key('3') in db3)
    self.assertTrue(Key('3') in tdb)

    self.assertEqual(db3[Key('3')], '3')
    self.assertFalse(Key('3') in db1)
    self.assertFalse(Key('3') in db2)

    self.assertEqual(tdb[Key('3')], '3')
    self.assertEqual(db1[Key('3')], '3')
    self.assertEqual(db2[Key('3')], '3')
    self.assertEqual(db3[Key('3')], '3')

    del tdb[Key('1')]
    del tdb[Key('2')]
    del tdb[Key('3')]

    self.assertFalse(Key('1') in tdb)
    self.assertFalse(Key('2') in tdb)
    self.assertFalse(Key('3') in tdb)

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
          self.assertTrue(key in db)
          self.assertEqual(db[key], value)
        else:
          self.assertFalse(key in db)

      if correct_shard == shard:
        self.assertTrue(key in sdb)
        self.assertEqual(sdb[key], value)
      else:
        self.assertFalse(key in sdb)

    self.assertEqual(sumlens(dbs), 0)
    # test all correct.
    for i in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % i)
      shard = dbs[hash(key) % len(dbs)]
      checkFor(key, i, sdb)
      shard[key] = i
      checkFor(key, i, sdb, shard)
    self.assertEqual(sumlens(dbs), 1000)

    # ensure its in the same spots.
    for i in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % i)
      shard = dbs[hash(key) % len(dbs)]
      checkFor(key, i, sdb, shard)
      shard[key] = i
      checkFor(key, i, sdb, shard)
    self.assertEqual(sumlens(dbs), 1000)

    # ensure its in the same spots.
    for i in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % i)
      shard = dbs[hash(key) % len(dbs)]
      checkFor(key, i, sdb, shard)
      sdb[key] = i
      checkFor(key, i, sdb, shard)
    self.assertEqual(sumlens(dbs), 1000)

    # ensure its in the same spots.
    for i in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % i)
      shard = dbs[hash(key) % len(dbs)]
      checkFor(key, i, sdb, shard)
      if i % 2 == 0:
        del shard[key]
      else:
        del sdb[key]
      checkFor(key, i, sdb)
    self.assertEqual(sumlens(dbs), 0)

    # try out adding it to the wrong shards.
    for i in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % i)
      incorrect_shard = dbs[(hash(key) + 1) % len(dbs)]
      checkFor(key, i, sdb)
      incorrect_shard[key] = i
      checkFor(key, i, sdb, incorrect_shard)
    self.assertEqual(sumlens(dbs), 1000)

    # ensure its in the same spots.
    for i in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % i)
      incorrect_shard = dbs[(hash(key) + 1) % len(dbs)]
      checkFor(key, i, sdb, incorrect_shard)
      incorrect_shard[key] = i
      checkFor(key, i, sdb, incorrect_shard)
    self.assertEqual(sumlens(dbs), 1000)

    # this wont do anything
    for i in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % i)
      incorrect_shard = dbs[(hash(key) + 1) % len(dbs)]
      checkFor(key, i, sdb, incorrect_shard)
      del sdb[key]
      checkFor(key, i, sdb, incorrect_shard)
    self.assertEqual(sumlens(dbs), 1000)

    # this will place it correctly.
    for i in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % i)
      incorrect_shard = dbs[(hash(key) + 1) % len(dbs)]
      correct_shard = dbs[(hash(key)) % len(dbs)]
      checkFor(key, i, sdb, incorrect_shard)
      sdb[key] = i
      del incorrect_shard[key]
      checkFor(key, i, sdb, correct_shard)
    self.assertEqual(sumlens(dbs), 1000)

    # this will place it correctly.
    for i in range(0, 1000):
      key = Key('/fdasfdfdsafdsafdsa/%d' % i)
      correct_shard = dbs[(hash(key)) % len(dbs)]
      checkFor(key, i, sdb, correct_shard)
      del sdb[key]
      checkFor(key, i, sdb)
    self.assertEqual(sumlens(dbs), 0)

    self.test_simple([sdb])



