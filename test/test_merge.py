import unittest
import hashlib
import nanotime

from dronestore.util import serial
from dronestore.model import *
from dronestore.merge import *
from dronestore.attribute import *

class PersonM(Model):
  first = StringAttribute(default="Firstname", strategy=LatestStrategy)
  last = StringAttribute(default="Lastname", strategy=LatestStrategy)
  phone = StringAttribute(default="N/A", strategy=LatestStrategy)
  age = IntegerAttribute(default=0, strategy=MaxStrategy)
  gender = StringAttribute(strategy=LatestObjectStrategy)

  def __str__(self):
    return '%s %s %s #%s age %d gender %s' % \
      (self.key, self.first, self.last, self.phone, self.age, self.gender)

class MergeTests(unittest.TestCase):

  def subtest_assert_blank_person(self, person):
    self.assertTrue(person.created.nanoseconds() == 0)
    self.assertTrue(person.committed.nanoseconds() == 0)
    self.assertTrue(person.version.isBlank)

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
    self.assertEqual(p.version.type, PersonM.__dstype__)
    self.assertEqual(p.version.hash, p.computedHash())
    self.assertEqual(p.version.parent, parentHash)

  def subtest_commits(self, p1, p2, diff=None):
    self.assertEqual(p1.isDirty(), p2.isDirty())
    self.assertEqual(p1.isCommitted(), p2.isCommitted())
    self.assertEqual(p1.version.type, p2.version.type)

    check = self.assertNotEqual if diff is not None else self.assertEqual
    check(p1.version, p2.version)
    check(p1.version.hash, p2.version.hash)
    check(p1.version.hash, p2.computedHash())
    check(p2.version.hash, p1.computedHash())

    if diff is None:
      diff = []

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
    a1 = PersonM('Tesla')
    a2 = PersonM('Tesla')
    self.assertEqual(a1.key, Key('/PersonM:Tesla'))
    self.assertEqual(a2.key, Key('/PersonM:Tesla'))
    self.assertEqual(a1.__dstype__, 'PersonM')
    self.assertEqual(a2.__dstype__, 'PersonM')
    self.subtest_assert_blank_person(a1)
    self.subtest_assert_blank_person(a2)

    a1.age = 52
    a1.commit()
    print 'committed a1', a1.version.hash

    a2.age = 52
    a2.commit()
    print 'committed a2', a2.version.hash

    self.subtest_committed(a1)
    self.subtest_committed(a2)
    self.subtest_commits(a1, a2)

    parentHash1 = a1.version.hash
    parentHash2 = a2.version.hash
    self.assertEqual(parentHash1, parentHash2)

    a1.first = 'Nikola'
    a1.last = 'Tesla'
    a1.phone = '7777777777'
    a1.commit()
    print 'committed a1', a1.version.hash

    a2.first = 'Nikola'
    a2.last = 'Tesla'
    a2.phone = '7777777777'
    a2.commit()
    print 'committed a2', a2.version.hash

    self.subtest_committed(a1, parentHash=parentHash1)
    self.subtest_committed(a2, parentHash=parentHash2)
    self.subtest_commits(a1, a2, diff=[])

    parentHash1 = a1.version.hash
    parentHash2 = a2.version.hash
    self.assertNotEqual(parentHash1, parentHash2)

    a1.age = 53
    self.assertTrue(a1.isDirty())
    self.assertTrue(a1.isCommitted())

    a1.commit()
    print 'committed a1', a1.version.hash

    a2.gender = 'Male'
    self.assertTrue(a2.isDirty())
    self.assertTrue(a2.isCommitted())

    a2.commit()
    print 'committed a2', a2.version.hash

    self.subtest_committed(a1, parentHash=parentHash1)
    self.subtest_committed(a2, parentHash=parentHash2)
    self.subtest_commits(a1, a2, diff=['age', 'gender'])

    parentHash1 = a1.version.hash
    a1.merge(a2)
    print 'a1 merged a2', a1.version.hash

    self.subtest_committed(a1, parentHash=parentHash1)
    self.subtest_committed(a2, parentHash=parentHash2)
    self.subtest_commits(a1, a2, diff=['age'])

    parentHash2 = a2.version.hash
    a2.merge(a1)
    print 'a2 merged a1', a2.version.hash

    self.subtest_committed(a1, parentHash=parentHash1)
    self.subtest_committed(a2, parentHash=parentHash2)
    self.subtest_commits(a1, a2)

  def test_merge_latest_object(self):
    a1 = PersonM('A')
    a2 = PersonM('A')
    a3 = PersonM('A')
    a4 = PersonM('A')

    a1.gender = 'Male'
    a2.gender = 'Female'
    a3.gender = 'Male'
    a4.gender = 'Neither'

    a1.commit()
    a2.commit()
    a3.commit()
    a4.commit()

    a3.merge(a4)
    a4.merge(a3)
    self.assertEqual(a3.gender, 'Neither')
    self.assertEqual(a4.gender, 'Neither')

    a1.merge(a2)
    a2.merge(a1)
    self.assertEqual(a1.gender, 'Female') # 2 was committed later.
    self.assertEqual(a2.gender, 'Female') # 1 was merged later, but was 2's val.

    a1.merge(a3)
    a3.merge(a1)
    self.assertEqual(a1.gender, 'Female') # 1 was committed after 3 (marge)
    self.assertEqual(a3.gender, 'Female') # 1 was committed after 3 (merge)

    a4.merge(a3)
    a3.merge(a4)
    self.assertEqual(a3.gender, 'Female')
    self.assertEqual(a4.gender, 'Female')

    self.assertEqual(a1.version.hash, a2.version.hash)
    self.assertEqual(a1.version.hash, a3.version.hash)
    self.assertEqual(a1.version.hash, a4.version.hash)


  def test_merge_latest_attribute(self):
    a1 = PersonM('A')
    a2 = PersonM('A')
    a3 = PersonM('A')
    a4 = PersonM('A')

    a1.first = 'first1'
    a2.first = 'first2'
    a3.first = 'first3'
    a4.first = 'first4'

    a1.commit()
    a2.commit()
    a3.commit()
    a4.commit()

    a4.last = 'last4'
    a3.last = 'last3'
    a2.last = 'last2'
    a1.last = 'last1'

    a1.commit()
    a2.commit()
    a3.commit()
    a4.commit()

    a1.phone = 'phone1'
    a4.phone = 'phone4'
    a3.phone = 'phone3'
    a2.phone = 'phone2'

    a1.commit()
    a2.commit()
    a3.commit()
    a4.commit()

    def check(instance, first, last, phone):
      self.assertEqual(instance.first, first)
      self.assertEqual(instance.last, last)
      self.assertEqual(instance.phone, phone)

    check(a1, 'first1', 'last1', 'phone1')
    check(a2, 'first2', 'last2', 'phone2')
    check(a3, 'first3', 'last3', 'phone3')
    check(a4, 'first4', 'last4', 'phone4')

    a3.merge(a4)
    check(a3, 'first4', 'last3', 'phone3')

    a4.merge(a3)
    check(a4, 'first4', 'last3', 'phone3')

    a1.merge(a2)
    check(a1, 'first2', 'last1', 'phone2')

    a2.merge(a1)
    check(a2, 'first2', 'last1', 'phone2')

    a1.merge(a3)
    check(a1, 'first4', 'last1', 'phone2')

    a3.merge(a1)
    check(a3, 'first4', 'last1', 'phone2')

    a4.merge(a3)
    check(a4, 'first4', 'last1', 'phone2')

    a3.merge(a4)
    check(a3, 'first4', 'last1', 'phone2')

    a1.merge(a4)
    a2.merge(a4)
    a3.merge(a4)

    check(a1, 'first4', 'last1', 'phone2')
    check(a2, 'first4', 'last1', 'phone2')
    check(a3, 'first4', 'last1', 'phone2')
    check(a4, 'first4', 'last1', 'phone2')

    self.assertEqual(a1.version.hash, a2.version.hash)
    self.assertEqual(a1.version.hash, a3.version.hash)
    self.assertEqual(a1.version.hash, a4.version.hash)


  def test_merge_max(self):
    a1 = PersonM('A')
    a2 = PersonM('A')
    a3 = PersonM('A')
    a4 = PersonM('A')

    a1.age = 11
    a2.age = 22
    a3.age = 33
    a4.age = 44

    a1.commit()
    a2.commit()
    a3.commit()
    a4.commit()

    a1.merge(a2)
    a2.merge(a1)
    self.assertEqual(a1.age, 22) # 2 was committed later.
    self.assertEqual(a2.age, 22) # 1 was merged later, but was 2's val.

    a1.merge(a3)
    a3.merge(a1)
    self.assertEqual(a1.age, 33) # 1 was committed after 3 (marge)
    self.assertEqual(a3.age, 33) # 1 was committed after 3 (merge)

    a4.merge(a3)
    a3.merge(a4)
    self.assertEqual(a3.age, 44)
    self.assertEqual(a4.age, 44)

    a1.merge(a4)
    a2.merge(a4)
    a3.merge(a4)

    a4.merge(a2)
    a4.merge(a1)
    a4.merge(a3)

    self.assertEqual(a1.age, 44)
    self.assertEqual(a2.age, 44)
    self.assertEqual(a3.age, 44)
    self.assertEqual(a4.age, 44)

    self.assertEqual(a1.version.hash, a2.version.hash)
    self.assertEqual(a1.version.hash, a3.version.hash)
    self.assertEqual(a1.version.hash, a4.version.hash)

if __name__ == '__main__':
  unittest.main()
