
import unittest
import nanotime

from dronestore.model import *
from dronestore.attribute import *


class AttributeTests(unittest.TestCase):

  def subtest_attribute(self, attrtype, **kwds):
    error = lambda: Attribute(merge_strategy='bogus')
    self.assertRaises(TypeError, error)

    a = attrtype(name='a', **kwds)
    self.assertEqual(a.name, 'a')
    self.assertEqual(a._attr_name(), '_a')
    self.assertEqual(a.default, None)
    self.assertEqual(a.default_value(), None)
    self.assertTrue(a.empty(None))
    self.assertFalse(a.required)
    self.assertTrue(isinstance(a.mergeStrategy, merge.MergeStrategy))

    # setup the binding to an object
    class Attributable(object):
      pass

    m = Attributable()
    m._attributes = {'a':a}
    a._attr_config(Attributable, 'a')

    self.assertEqual(a._attr_name(), '_a')
    self.assertEqual(m._attributes['a'], a)

    def testSet(value, testval=None):
      a.__set__(m, value)
      if not testval:
        testval = value
      self.assertEqual(a.__get__(m, object), testval)

    return testSet

  def test_basicattributes(self):
    test = self.subtest_attribute(Attribute)
    test(5, '5')
    test(5.2, '5.2')
    test(self, str(self))
    test('5')
    test(None)

    test = self.subtest_attribute(StringAttribute)
    test(5, '5')
    test(5.2, '5.2')
    test(self, str(self))
    test('5')
    test(None)
    self.assertRaises(ValueError, test, '5\n\n\nfdsijhfdiosahfdsajfdias')

    test = self.subtest_attribute(StringAttribute, multiline=True)
    test(5, '5')
    test(5.2, '5.2')
    test(self, str(self))
    test('5')
    test('5\n\n\nfdsijhfdiosahfdsajfdias')
    test(None)

    test = self.subtest_attribute(TextAttribute, multiline=True)
    test(5, '5')
    test(5.2, '5.2')
    test(self, str(self))
    test('5')
    test('5\n\n\nfdsijhfdiosahfdsajfdias')
    test(None)

    test = self.subtest_attribute(IntegerAttribute)
    test(5)
    test(5.2, 5)
    self.assertRaises(TypeError, test, self)
    test('5', 5)
    self.assertRaises(TypeError, test, '5a')
    test(None)

    test = self.subtest_attribute(FloatAttribute)
    test(5, 5.0)
    test(5.2)
    self.assertRaises(TypeError, test, self)
    test('5', 5.0)
    self.assertRaises(TypeError, test, '5a')
    test(None)

    test = self.subtest_attribute(BooleanAttribute)
    test(5, True)
    test(5.2, True)
    test(self, True)
    test('5', True)
    test('5a', True)
    test(True)
    test(False)
    test(None)


  def test_keyattributes(self):

    test = self.subtest_attribute(KeyAttribute)
    test(5, Key(5))
    test(5.2, Key(5.2))
    test(self, Key(self))
    test('5', Key('5'))
    self.assertRaises(ValueError, test, '5\n\n\nfdsijhfdiosahfdsajfdias')
    test(None)

    test = self.subtest_attribute(KeyAttribute, type='Herp')
    self.assertRaises(ValueError, test, 5)
    self.assertRaises(ValueError, test, 5.2)
    self.assertRaises(ValueError, test, self)
    self.assertRaises(ValueError, test, '5')
    self.assertRaises(ValueError, test, '5\n\n\nfdsijhfdiosahfdsajfdias')
    test(None)
    self.assertRaises(ValueError, test, '/Herp')
    test(Key('/Herp:Derp'))
    self.assertRaises(ValueError, test, '/Herp:Derp/Lerp')

    test = self.subtest_attribute(KeyAttribute, parent='/Herp:Derp')
    self.assertRaises(ValueError, test, 5)
    self.assertRaises(ValueError, test, 5.2)
    self.assertRaises(ValueError, test, self)
    self.assertRaises(ValueError, test, '5')
    self.assertRaises(ValueError, test, '5\n\n\nfdsijhfdiosahfdsajfdias')
    test(None)
    self.assertRaises(ValueError, test, '/Herp')
    self.assertRaises(ValueError, test, '/Herp:Derp')
    test(Key('/Herp:Derp/Lerp'))
    self.assertRaises(ValueError, test, '/Merp/Herp:Derp/Lerp')

    test = self.subtest_attribute(KeyAttribute, ancestor='/Herp')
    self.assertRaises(ValueError, test, 5)
    self.assertRaises(ValueError, test, 5.2)
    self.assertRaises(ValueError, test, self)
    self.assertRaises(ValueError, test, '5')
    self.assertRaises(ValueError, test, '5\n\n\nfdsijhfdiosahfdsajfdias')
    test(None)
    self.assertRaises(ValueError, test, '/Herp')
    test(Key('/Herp/Derp'))
    test(Key('/Herp/Derp/Lerp'))
    self.assertRaises(ValueError, test, '/Merp/Herp/Derp/Lerp')

    test = self.subtest_attribute(KeyAttribute, descendant='/Herp/Derp/Lerp')
    self.assertRaises(ValueError, test, 5)
    self.assertRaises(ValueError, test, 5.2)
    self.assertRaises(ValueError, test, self)
    self.assertRaises(ValueError, test, '5')
    self.assertRaises(ValueError, test, '5\n\n\nfdsijhfdiosahfdsajfdias')
    test(None)
    test(Key('/Herp'))
    test(Key('/Herp/Derp'))
    self.assertRaises(ValueError, test, '/Herp/Derp/Lerp')
    self.assertRaises(ValueError, test, '/Merp/Herp/Derp')
    self.assertRaises(ValueError, test, '/Merp/Herp/Derp/Lerp')


  def test_other(self):

    test = self.subtest_attribute(TimeAttribute)
    test(5, nanotime.nanotime(5))
    test(5.2, nanotime.nanotime(5.2))
    self.assertRaises(TypeError, test, self)
    self.assertRaises(TypeError, test, '5')
    self.assertRaises(TypeError, test, '5a')
    test(nanotime.seconds(1000))
    test(None)

    test = self.subtest_attribute(DateTimeAttribute)
    self.assertRaises(TypeError, test, 5)
    self.assertRaises(TypeError, test, 5.2)
    self.assertRaises(TypeError, test, self)
    self.assertRaises(ValueError, test, '5')
    self.assertRaises(ValueError, test, '5a')
    test(datetime.datetime.now())
    test(None)
    d = datetime.datetime(2011, 7, 12, 13, 19, 29, 151455)
    test(str(d), d)
    d = datetime.datetime(2011, 7, 12, 13, 19, 29, 0)
    test(str(d), d)

    test = self.subtest_attribute(ListAttribute)
    self.assertRaises(TypeError, test, 5, ['5'])
    self.assertRaises(TypeError, test, 5.2, ['5.2'])
    self.assertRaises(TypeError, test, self, [str(self)])
    test('5', ['5'])
    test('5a', ['5', 'a'])
    test([])
    test(['fdgfds', 'gfdsgfds', 'gfdsgfds', 'gfdsgfds'])
    test([4214, 321, 43, 21], ['4214', '321', '43', '21'])
    test(xrange(0, 10), map(str, range(0, 10)))
    test(None)

    test = self.subtest_attribute(DictAttribute)
    self.assertRaises(TypeError, test, 5)
    self.assertRaises(TypeError, test, 5.2)
    self.assertRaises(TypeError, test, self)
    self.assertRaises(TypeError, test, '5')
    self.assertRaises(TypeError, test, '5a')
    test({})
    test({'a':'b'})
    test({'1213':3214}, {'1213':'3214'})
    test({1213:3214}, {'1213':'3214'})
    test(None)

if __name__ == '__main__':
  unittest.main()
