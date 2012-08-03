
import copy
import random
import unittest

from .util import RandomGen

from dronestore.util.serial import SerialRepresentation as SR

class TestSerial(unittest.TestCase):

  def test_basic(self):
    eq = self.assertEqual

    data = {'a':1, 'b':2, 'c':3, 'd':4}

    sr1 = SR()
    for k in data:
      self.assertFalse(k in sr1)
      sr1[k] = data[k]
      self.assertTrue(k in sr1)
      self.assertEqual(sr1[k], data[k])

    sr2 = SR(copy.deepcopy(data))
    self.assertEqual(sr1, sr2)

    self.assertEqual(len(sr1), len(sr2))
    self.assertEqual(len(sr1), len(data))

    for k in data:
      self.assertTrue(k in sr1)
      self.assertTrue(k in sr2)
      del sr1[k]
      del sr2[k]
      self.assertFalse(k in sr1)
      self.assertFalse(k in sr2)

    self.assertEqual(len(sr1), 0)
    self.assertEqual(len(sr1), len(sr2))

  def __subtest_conversions(self, data):
    print 'Testing', data
    self.assertEqual(SR(data), SR(data))
    self.assertEqual(SR(data).data(), data)

  def test_conversions(self):
    self.__subtest_conversions({})
    self.__subtest_conversions({'a':1})
    self.__subtest_conversions({'a':1, 'b':2})
    self.__subtest_conversions(RandomGen.randomDict())
    self.__subtest_conversions(RandomGen.randomDict())
    self.__subtest_conversions(RandomGen.randomDict())

if __name__ == '__main__':
  unittest.main()
