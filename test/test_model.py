import unittest
import hashlib

from dronestore.util import nanotime
from dronestore.util import serial
from dronestore.model import *


class TestKey(unittest.TestCase):

  def __subtest_basic(self, string):
    self.assertEqual(Key(string)._str, string)
    self.assertEqual(Key(string), Key(string))
    self.assertEqual(str(Key(string)), string)
    self.assertEqual(repr(Key(string)), string)

    self.assertRaises(TypeError, cmp, Key(string), string)

  def test_basic(self):
    self.__subtest_basic('')
    self.__subtest_basic('abcde')
    self.__subtest_basic('disahfidsalfhduisaufidsail')
    self.__subtest_basic('/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/')
    self.__subtest_basic(u'4215432143214321432143214321')


  def test_ancestry(self):
    k1 = Key('/A/B/C')
    self.assertEqual(k1._str, '/A/B/C')

    k2 = Key('/A/B/C/D')
    self.assertEqual(k2._str, '/A/B/C/D')
    self.assertTrue(k1.isAncestorOf(k2))
    self.assertEqual(k1.child('D'), k2)
    self.assertEqual(k1, k2.parent())

    self.assertRaises(TypeError, k1.isAncestorOf, str(k2))


  def test_random(self):
    keys = set()
    for i in range(0, 1000):
      random = Key.randomKey()
      self.assertFalse(random in keys)
      keys.add(random)
    self.assertEqual(len(keys), 1000)


class TestVersion(unittest.TestCase):

  def test_blank(self):
    blank = Version()
    self.assertEqual(blank.hash(), Version.BLANK_HASH)
    self.assertEqual(blank.type(), '')
    self.assertEqual(blank.shortHash(5), Version.BLANK_HASH[0:5])
    self.assertEqual(blank.committed(), nanotime.NanoTime(0))
    self.assertEqual(blank.parent(), Version.BLANK_HASH)

    self.assertEqual(blank, Version())
    self.assertTrue(blank.isBlank())


  def test_creation(self):

    h1 = hashlib.sha1('derp').hexdigest()
    h2 = hashlib.sha1('herp').hexdigest()
    now = nanotime.now()

    sr = serial.SerialRepresentation()
    sr['hash'] = h1
    sr['parent'] = h2
    sr['committed'] = now.nanoseconds()
    sr['attributes'] = {'str' : 'derp'}
    sr['type'] = 'Hurr'

    v = Version(sr)
    self.assertEqual(v.type(), 'Hurr')
    self.assertEqual(v.hash(), h1)
    self.assertEqual(v.parent(), h2)
    self.assertEqual(v.committed(), now)
    self.assertEqual(v.shortHash(5), h1[0:5])
    self.assertEqual(v.attribute('str'), 'derp')
    self.assertEqual(v['str'], 'derp')
    self.assertEqual(hash(v), hash(h1))
    self.assertEqual(v, Version(sr))
    self.assertFalse(v.isBlank())

    self.assertRaises(KeyError, v.attribute, 'fdsafda')
    self.assertRaises(TypeError, cmp, v, 'fdsafda')

  def test_raises(self):
    def assertCreationRaises(sr):
      self.assertRaises(ValueError, Version, sr)

    sr = serial.SerialRepresentation()
    self.assertRaises(ValueError, Version, sr)
    sr['hash'] = 'a'
    assertCreationRaises(sr)
    sr['parent'] = 'b'
    assertCreationRaises(sr)
    sr['committed'] = nanotime.now().nanoseconds()
    assertCreationRaises(sr)
    sr['attributes'] = {'str' : 'derp'}
    assertCreationRaises(sr)
    sr['type'] = 'Hurr'
    Version(sr)

