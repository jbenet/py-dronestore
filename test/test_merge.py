import unittest
import hashlib

from dronestore.util import nanotime
from dronestore.util import serial
from dronestore.model import *




class TestPerson(Model):
  first = StringAttribute(default="Firstname")
  last = StringAttribute(default="Lastname")
  phone = StringAttribute(default="N/A")
  age = IntegerAttribute(default=0)
  gender = StringAttribute()

class MergeTests(unittest.TestCase):

  def subtest_assert_blank_person(self, person):
    self.assertTrue(person.created is None)
    self.assertTrue(person.updated is None)
    self.assertTrue(person.version.isBlank())

    self.assertTrue(person.isDirty())
    self.assertFalse(person.isPersisted())
    self.assertFalse(person.isCommitted())

    self.assertEqual(person.first, 'Firstname')
    self.assertEqual(person.last, 'Lastname')
    self.assertEqual(person.phone, 'N/A')
    self.assertEqual(person.age, 0)
    self.assertEqual(person.gender, None)


  def subtest_committed(self, p, parentHash=Version.BLANK_HASH):
    self.assertFalse(p.isDirty())
    self.assertTrue(p.isCommitted())
    self.assertEqual(p.version.type(), TestPerson.__dstype__)
    self.assertEqual(p.version.hash(), p.computedHash())
    self.assertEqual(p.version.parent(), parentHash)

  def subtest_commits(self, p1, p2, diff = []):
    self.assertEqual(p1.isDirty(), p2.isDirty())
    self.assertEqual(p1.isCommitted(), p2.isCommitted())
    self.assertEqual(p1.version.type(), p2.version.type())

    check = self.assertNotEqual if diff else self.assertEqual
    check(p1.version, p2.version)
    check(p1.version.hash(), p2.version.hash())
    check(p1.version.hash(), p2.computedHash())
    check(p2.version.hash(), p1.computedHash())

    for attr_name in p1.attributes():
      check = self.assertNotEqual if attr_name in diff else self.assertEqual

      attr1 = getattr(p1, attr_name)
      attr2 = getattr(p2, attr_name)
      check(attr1, attr2)

      attr1 = p1.version.attributeValue(attr_name)
      attr2 = p2.version.attributeValue(attr_name)
      check(attr1, attr2)

      attr1 = p1.version.attribute(attr_name)['value']
      attr2 = p2.version.attribute(attr_name)['value']
      check(attr1, attr2)


  def test_basic(self):
    a1 = TestPerson('A')
    a2 = TestPerson('A')
    self.assertEqual(a1.key, Key('/TestPerson/A'))
    self.assertEqual(a2.key, Key('/TestPerson/A'))
    self.assertEqual(a1.__dstype__, 'TestPerson')
    self.assertEqual(a2.__dstype__, 'TestPerson')
    self.subtest_assert_blank_person(a1)
    self.subtest_assert_blank_person(a2)

    a1.first = 'Nikola'
    a1.last = 'Tesla'
    a1.phone = '7777777777'
    a1.age = 52

    a2.first = 'Nikola'
    a2.last = 'Tesla'
    a2.phone = '7777777777'
    a2.age = 52

    a1.commit()
    print 'committed a1', a1.version.hash()
    a2.commit()
    print 'committed a2', a2.version.hash()

    self.subtest_committed(a1)
    self.subtest_committed(a2)
    self.subtest_commits(a1, a2)

    parentHash1 = a1.version.hash()
    parentHash2 = a2.version.hash()
    self.assertEqual(parentHash1, parentHash2)

    a1.age = 53
    self.assertTrue(a1.isDirty())
    self.assertTrue(a1.isCommitted())

    a1.commit()
    print 'committed a1', a1.version.hash()

    a2.gender = 'Male'
    self.assertTrue(a2.isDirty())
    self.assertTrue(a2.isCommitted())

    a2.commit()
    print 'committed a2', a1.version.hash()

    self.subtest_committed(a1, parentHash=parentHash1)
    self.subtest_committed(a2, parentHash=parentHash2)
    self.subtest_commits(a1, a2, diff=['age', 'gender'])



