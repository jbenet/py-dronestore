import unittest
import hashlib
import nanotime

from dronestore.util import serial
from dronestore.util import fasthash
from dronestore.model import *
from dronestore.attribute import *

from .util import RandomGen


class Person(Model):
  first = StringAttribute(default="Firstname")
  last = StringAttribute(default="Lastname")
  phone = StringAttribute(default="N/A")
  age = IntegerAttribute(default=0)
  gender = StringAttribute()

  def validate(self):
    super(Person, self).validate()

    if self.gender == self.first:
      raise ValueError('Sorry, a gender is not an acceptable first name!')



class KeyTests(unittest.TestCase):

  def __subtest_basic(self, string):
    fixedString = Key.removeDuplicateSlashes(string)
    self.assertEqual(Key(string)._string, fixedString)
    self.assertEqual(Key(string), Key(string))
    self.assertEqual(str(Key(string)), fixedString)
    self.assertEqual(repr(Key(string)), "Key('%s')" % fixedString)
    self.assertEqual(Key(string).name, fixedString.rsplit('/')[-1])
    self.assertEqual(Key(string), eval(repr(Key(string))))

    self.assertRaises(TypeError, cmp, Key(string), string)

    split = fixedString.split('/')
    self.assertEqual(Key('/'.join(split[:-1])), Key(string).parent)

  def test_basic(self):
    self.__subtest_basic('')
    self.__subtest_basic('abcde')
    self.__subtest_basic('disahfidsalfhduisaufidsail')
    self.__subtest_basic('/fdisahfodisa/fdsa/fdsafdsafdsafdsa/fdsafdsa/')
    self.__subtest_basic(u'4215432143214321432143214321')
    self.__subtest_basic('/fdisaha////fdsa////fdsafdsafdsafdsa/fdsafdsa/')


  def test_ancestry(self):
    k1 = Key('/A/B/C')
    k2 = Key('/A/B/C/D')

    self.assertEqual(k1._string, '/A/B/C')
    self.assertEqual(k2._string, '/A/B/C/D')
    self.assertTrue(k1.isAncestorOf(k2))
    self.assertTrue(k2.isDescendantOf(k1))
    self.assertTrue(Key('/A').isAncestorOf(k2))
    self.assertTrue(Key('/A').isAncestorOf(k1))
    self.assertFalse(Key('/A').isDescendantOf(k2))
    self.assertFalse(Key('/A').isDescendantOf(k1))
    self.assertTrue(k2.isDescendantOf(Key('/A')))
    self.assertTrue(k1.isDescendantOf(Key('/A')))
    self.assertFalse(k2.isAncestorOf(Key('/A')))
    self.assertFalse(k1.isAncestorOf(Key('/A')))
    self.assertFalse(k2.isAncestorOf(k2))
    self.assertFalse(k1.isAncestorOf(k1))
    self.assertEqual(k1.child('D'), k2)
    self.assertEqual(k1, k2.parent)

    self.assertRaises(TypeError, k1.isAncestorOf, str(k2))
    self.assertEqual(k1.parent, Key('/A/B'))
    self.assertEqual(k2.parent, Key('/A/B/C'))
    self.assertEqual(k2.parent, k1)

  def test_hashing(self):

    def randomKey():
      return Key('/herp/' + RandomGen.randomString() + '/derp')

    keys = {}

    for i in range(0, 200):
      key = randomKey()
      while key in keys.values():
        key = randomKey()

      hstr = str(hash(key))
      self.assertFalse(hstr in keys)
      keys[hstr] = key

    for key in keys.values():
      hstr = str(hash(key))
      self.assertTrue(hstr in keys)
      self.assertEqual(key, keys[hstr])

  def test_random(self):
    keys = set()
    for i in range(0, 1000):
      random = Key.randomKey()
      self.assertFalse(random in keys)
      keys.add(random)
    self.assertEqual(len(keys), 1000)





class VersionTests(unittest.TestCase):

  def test_blank(self):
    blank = Version(Key('/BLANK'))
    self.assertEqual(blank.hash, Version.BLANK_HASH)
    self.assertEqual(blank.type, '')
    self.assertEqual(blank.shortHash(5), Version.BLANK_HASH[0:5])
    self.assertEqual(blank.committed, nanotime.nanotime(0))
    self.assertEqual(blank.created, nanotime.nanotime(0))
    self.assertEqual(blank.parent, Version.BLANK_HASH)

    self.assertEqual(blank, Version(Key('/BLANK')))
    self.assertTrue(blank.isBlank)


  def test_creation(self):

    h1 = hashlib.sha1('derp').hexdigest()
    h2 = hashlib.sha1('herp').hexdigest()
    now = nanotime.now()

    sr = serial.SerialRepresentation()
    sr['key'] = '/A'
    sr['hash'] = h1
    sr['parent'] = h2
    sr['created'] = now.nanoseconds()
    sr['committed'] = now.nanoseconds()
    sr['attributes'] = {'str' : {'value' : 'derp'} }
    sr['type'] = 'Hurr'

    v = Version(sr)
    self.assertEqual(v.type, 'Hurr')
    self.assertEqual(v.hash, h1)
    self.assertEqual(v.parent, h2)
    self.assertEqual(v.created, now)
    self.assertEqual(v.committed, now)
    self.assertEqual(v.shortHash(5), h1[0:5])
    self.assertEqual(v.attributeValue('str'), 'derp')
    self.assertEqual(v.attribute('str')['value'], 'derp')
    self.assertEqual(v['str']['value'], 'derp')
    self.assertEqual(hash(v), hash(fasthash.hash(h1)))
    self.assertEqual(v, Version(sr))
    self.assertFalse(v.isBlank)

    self.assertRaises(KeyError, v.attribute, 'fdsafda')
    self.assertRaises(TypeError, cmp, v, 'fdsafda')

  def test_raises(self):

    sr = serial.SerialRepresentation()
    self.assertRaises(ValueError, Version, sr)
    sr['key'] = '/A'
    self.assertRaises(ValueError, Version, sr)
    sr['hash'] = 'a'
    self.assertRaises(ValueError, Version, sr)
    sr['parent'] = 'b'
    self.assertRaises(ValueError, Version, sr)
    sr['created'] = nanotime.now().nanoseconds()
    self.assertRaises(ValueError, Version, sr)
    sr['committed'] = 0
    self.assertRaises(ValueError, Version, sr)
    sr['committed'] = sr['created']
    self.assertRaises(ValueError, Version, sr)
    sr['attributes'] = {'str' : 'derp'}
    self.assertRaises(ValueError, Version, sr)
    sr['type'] = 'Hurr'
    Version(sr)


  def test_model(self):

    h1 = hashlib.sha1('derp').hexdigest()
    h2 = hashlib.sha1('herp').hexdigest()

    attrs = {'first' : {'value':'Herp'}, \
             'last' : {'value':'Derp'}, \
             'phone' : {'value': '123'}, \
             'age' : {'value': 19}, \
             'gender' : {'value' : 'Male'}}

    sr = serial.SerialRepresentation()
    sr['key'] = '/Person:PersonA'
    sr['hash'] = h1
    sr['parent'] = h2
    sr['created'] = nanotime.now().nanoseconds()
    sr['committed'] = sr['created']
    sr['attributes'] = attrs
    sr['type'] = 'Person'

    ver = Version(sr)
    instance = Person(ver)

    self.assertEqual(instance.__dstype__, ver.type)
    self.assertEqual(instance.version, ver)
    self.assertFalse(instance.isDirty())
    self.assertTrue(instance.isPersisted())
    self.assertTrue(instance.isCommitted())

    self.assertEqual(instance.key, Key('/Person:PersonA'))
    self.assertEqual(instance.first, 'Herp')
    self.assertEqual(instance.last, 'Derp')
    self.assertEqual(instance.phone, '123')
    self.assertEqual(instance.age, 19)
    self.assertEqual(instance.gender, 'Male')


class ModelTests(unittest.TestCase):

  def subtest_assert_uncommitted(self, instance):
    self.assertTrue(instance.created == 0)
    self.assertTrue(instance.committed == 0)
    self.assertTrue(instance.version.isBlank)

    self.assertTrue(instance.isDirty())
    self.assertFalse(instance.isPersisted())
    self.assertFalse(instance.isCommitted())

  def test_basic(self):

    now = nanotime.now()

    a = Model('A')
    self.assertEqual(Model.key, Key('/Model'))
    self.assertEqual(a.key, Key('/Model:A'))
    self.assertEqual(Model.key, a.key.path)
    self.assertEqual(Model.key.instance('A'), a.key)
    self.assertEqual(a.__dstype__, 'Model')
    self.assertEqual(Model.__dstype__, 'Model')
    self.subtest_assert_uncommitted(a)

    a.commit()
    created = a.version.created

    print 'committed', a.version.hash

    self.assertFalse(a.isDirty())
    self.assertTrue(a.isCommitted())
    self.assertEqual(a.version.type, Model.__dstype__)
    self.assertEqual(a.version.hash, a.computedHash())
    self.assertEqual(a.version.parent, Version.BLANK_HASH)
    self.assertEqual(a.version.created, created)
    self.assertEqual(a.created, a.version.created)
    self.assertEqual(a.committed, a.version.committed)
    self.assertTrue(a.created > now)
    self.assertTrue(a.committed > now)

    now = nanotime.now()
    a.commit()

    self.assertFalse(a.isDirty())
    self.assertTrue(a.isCommitted())
    self.assertEqual(a.version.type, Model.__dstype__)
    self.assertEqual(a.version.hash, a.computedHash())
    self.assertEqual(a.version.parent, Version.BLANK_HASH)
    self.assertEqual(a.version.created, created)
    self.assertEqual(a.created, a.version.created)
    self.assertEqual(a.committed, a.version.committed)
    self.assertTrue(a.created < now)
    self.assertTrue(a.committed < now) # didn't REALLY commit.


    a._isDirty = True
    self.assertTrue(a.isDirty())

    now = nanotime.now()
    a.commit()

    self.assertFalse(a.isDirty())
    self.assertTrue(a.isCommitted())
    self.assertEqual(a.version.type, Model.__dstype__)
    self.assertEqual(a.version.hash, a.computedHash())
    self.assertEqual(a.version.parent, Version.BLANK_HASH)
    self.assertEqual(a.version.created, created)
    self.assertEqual(a.created, a.version.created)
    self.assertEqual(a.committed, a.version.committed)
    self.assertTrue(a.created < now)
    self.assertTrue(a.committed < now) # didn't REALLY commit.


  def test_attributes(self):
    p = Person('HerpDerp')
    self.assertEqual(p.key, Key('/Person:HerpDerp'))
    self.assertEqual(p.first, 'Firstname')
    self.assertEqual(p.last, 'Lastname')
    self.assertEqual(p.phone, 'N/A')
    self.assertEqual(p.age, 0)
    self.assertEqual(p.gender, None)

    self.subtest_assert_uncommitted(p)

    p.first = 'Herp'
    p.last = 'Derp'
    p.phone = '1235674444'
    p.age = 120

    p.commit()
    print 'committed', p.version.shortHash(8)

    self.assertFalse(p.isDirty())
    self.assertTrue(p.isCommitted())
    self.assertEqual(p.version.type, Person.__dstype__)
    self.assertEqual(p.version.hash, p.computedHash())
    self.assertEqual(p.version.parent, Version.BLANK_HASH)

    self.assertEqual(p.first, 'Herp')
    self.assertEqual(p.last, 'Derp')
    self.assertEqual(p.phone, '1235674444')
    self.assertEqual(p.age, 120)
    self.assertEqual(p.gender, None)

    self.assertEqual(p.version.attributeValue('first'), 'Herp')
    self.assertEqual(p.version.attributeValue('last'), 'Derp')
    self.assertEqual(p.version.attributeValue('phone'), '1235674444')
    self.assertEqual(p.version.attributeValue('age'), 120)
    self.assertEqual(p.version.attributeValue('gender'), None)

    hash = p.version.hash
    p.first = 'Herpington'
    p.gender = 'Troll'
    p.commit()
    print 'committed', p.version.shortHash(8)

    self.assertFalse(p.isDirty())
    self.assertTrue(p.isCommitted())
    self.assertEqual(p.version.type, Person.__dstype__)
    self.assertEqual(p.version.hash, p.computedHash())
    self.assertEqual(p.version.parent, hash)

    self.assertEqual(p.first, 'Herpington')
    self.assertEqual(p.last, 'Derp')
    self.assertEqual(p.phone, '1235674444')
    self.assertEqual(p.age, 120)
    self.assertEqual(p.gender, 'Troll')

    self.assertEqual(p.version.attributeValue('first'), 'Herpington')
    self.assertEqual(p.version.attributeValue('last'), 'Derp')
    self.assertEqual(p.version.attributeValue('phone'), '1235674444')
    self.assertEqual(p.version.attributeValue('age'), 120)
    self.assertEqual(p.version.attributeValue('gender'), 'Troll')

    p.first = p.gender
    self.assertRaises(ValueError, p.commit)

    class Herp(Model):
      required = StringAttribute(required=True)

    h = Herp('Derp')
    self.assertRaises(ValueError, p.validate)
    self.assertRaises(ValueError, p.commit)

if __name__ == '__main__':
  unittest.main()
