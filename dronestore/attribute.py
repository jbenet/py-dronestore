
import datetime
import nanotime

import merge
import model

class Attribute(object):
  '''Attributes define and compose a Model. A Model can be seen as a collection
  of attributes.

  An Attribute primarily defines a name, an associated data type, and a
  particular merge strategy.

  Attributes can have other options, including defining a default value, and
  validation for the data they hold.
  '''
  data_type = str
  default_strategy = merge.LatestObjectStrategy

  def __init__(self, name=None, default=None, required=False, strategy=None):

    if not strategy:
      strategy = self.default_strategy
    strategy = strategy(self)

    if not isinstance(strategy, merge.MergeStrategy):
      raise TypeError('mergeStrategy does not inherit from %s' % \
        merge.MergeStrategy)

    strategy.attribute = self

    self.name = name
    self.default = default
    self.required = required
    self.mergeStrategy = strategy


  def _attr_config(self, model_class, attr_name):
    '''Configure attribute for a given model.'''
    self.__model__ = model_class
    if self.name is None:
      self.name = attr_name

  def _attr_name(self):
    '''Returns the attribute name within the model instance.'''
    return '_' + self.name

  def rawData(self, instance):
    if instance is None:
      return None

    try:
      return getattr(instance, self._attr_name())
    except AttributeError:
      return None

  def setRawData(self, instance, rawData):
    setattr(instance, self._attr_name(), rawData)
    instance._isDirty = True

  def __get__(self, instance, model_class):
    '''Descriptor to aid model instantiation.'''
    if instance is None:
      return self

    try:
      rawData = getattr(instance, self._attr_name())
      return self.loads(rawData['value'])
    except AttributeError:
      return self.default_value()

  def __set__(self, instance, value, default=False):
    '''Validate and Set the attribute on the model instance.'''
    if not default:
      value = self.validate(value)

    rawData = self.rawData(instance)
    if rawData is None:
      rawData = {}
      setattr(instance, self._attr_name(), rawData)

    # our attributes are idempotent, so if its the same, doesn't change state
    if 'value' in rawData:
      oldval = rawData['value']
      if value is None and oldval is None:
        return
      if value is not None and oldval is not None and oldval == value:
        return

    rawData['value'] = self.dumps(value)
    instance._isDirty = True
    self.mergeStrategy.setAttribute(instance, rawData, default=default)

  def default_value(self):
    '''The default value for a particular attribute.'''
    return self.default

  def validate(self, value):
    '''Assert that the provided value is compatible with this attribute.'''
    if self.empty(value):
      if self.required:
        raise ValueError('Attribute %s is required.' % self.name)

    if value is not None and not isinstance(value, self.data_type):
      try:
        value = self.data_type(value)
      except:
        errstr = 'value for attribute %s is of type %s, not %s'
        raise TypeError(errstr % (self.name, type(value), self.data_type))

    return value

  def empty(self, value):
    '''Simple check to determine if value is empty.'''
    return not value

  # the loads/dumps interface here is to provide an easy way for attributes
  # to store serialized information.
  def dumps(self, value):
    '''Returns value into raw data to store.'''
    return value

  def loads(self, raw):
    '''Converts raw data into value.'''
    return raw


class StringAttribute(Attribute):
  '''Keep compatibility with App Engine by using basestrings as well'''
  data_type = basestring

  def __init__(self, multiline=False, **kwds):
    super(StringAttribute, self).__init__(**kwds)
    self.multiline = multiline

  def validate(self, value):
    if value is not None and not isinstance(value, self.data_type):
      value = str(value)

    value = super(StringAttribute, self).validate(value)

    if not self.multiline and value and '\n' in value:
      raise ValueError('Attribute %s is not multi-line' % self.name)

    return value


class KeyAttribute(StringAttribute):
  '''Attribute to store Keys.'''
  data_type = model.Key

  def __init__(self, type=None, parent=None, ancestor=None, \
    descendant=None, **kwds):
    ktype = type
    type = self.__class__.__class__

    if 'multiline' not in kwds:
      kwds['multiline'] = False
    super(KeyAttribute, self).__init__(**kwds)

    if ktype and isinstance(ktype, type) and issubclass(ktype, model.Model):
      ktype = ktype.__dstype__

    self.type = str(ktype) if ktype else None
    self.parent = model.Key(parent) if parent else None
    self.ancestor = model.Key(ancestor) if ancestor else None
    self.descendant = model.Key(descendant) if descendant else None

  def validate(self, value):
    '''Ensures key value matches criteria.'''
    value = super(KeyAttribute, self).validate(value)
    errstr = "key '%s' for attribute '%s'" % (value, self.name)

    if value is not None:

      # make sure the key type matches.
      if self.type is not None and value.type != self.type:
        errstr += " must be of key type '%s'" % self.type
        raise ValueError(errstr)

      # make sure the parent key matches.
      if self.parent and value.parent != self.parent:
        errstr += " must have parent key '%s'" % self.parent
        raise ValueError(errstr)

      # make sure the ancestry matches.
      if self.ancestor and not self.ancestor.isAncestorOf(value):
        errstr += "must have ancestor key '%s'" % self.ancestor
        raise ValueError(errstr)

      # make sure the ancestry matches.
      if self.descendant and not self.descendant.isDescendantOf(value):
        errstr = " must be a descendant of key '%s'" % self.descendant
        raise ValueError(errstr)

    return value



class TextAttribute(StringAttribute):
  '''Attribute to store large amounts of text. Datastores should optimize.'''

  def __init__(self, **kwds):
    if 'multiline' not in kwds:
      kwds['multiline'] = True
    super(TextAttribute, self).__init__(**kwds)



class IntegerAttribute(Attribute):
  '''Integer Attribute'''
  data_type = int

  def validate(self, value):
    value = super(IntegerAttribute, self).validate(value)
    if value is None:
      return value

    if not isinstance(value, (int, long)) or isinstance(value, bool):
      raise ValueError('Attribute %s must be an int or long, not a %s'
                          % (self.name, type(value).__name__))

    if value < -0x8000000000000000 or value > 0x7fffffffffffffff:
      raise ValueError('Property %s must fit in 64 bits' % self.name)

    return value

  def empty(self, value):
    '''0 is not empty.'''
    return value is None



class FloatAttribute(Attribute):
  '''Floating point Attribute'''
  data_type = float

  def empty(self, value):
    '''0 is not empty.'''
    return value is None


class BooleanAttribute(Attribute):
  '''Boolean Attribute'''
  data_type = bool

  def empty(self, value):
    '''False is not empty.'''
    return value is None



class TimeAttribute(Attribute):
  '''Attribute to store nanosecond times.'''
  data_type = nanotime.nanotime

  # store the data as nanoseconds
  @classmethod
  def dumps(cls, nanotime_):
    if nanotime_ is None:
      return None
    return nanotime_.nanoseconds()

  @classmethod
  def loads(cls, nanoseconds):
    if nanoseconds is None:
      return None
    return nanotime.nanoseconds(nanoseconds)




class DateTimeAttribute(TimeAttribute):
  '''Attribute to store nanosecond times and return datetime objects.'''

  data_type = datetime.datetime

  @classmethod
  def dumps(cls, datetime_):
    if datetime_ is None:
      return None
    return datetime_.isoformat()

  @classmethod
  def loads(cls, iso_string):
    if iso_string is None:
      return None
    return cls._datetime_from_iso_string(iso_string)

  def __set__(self, instance, value, default=False):
    '''Set the attribute on the model instance.'''

    if isinstance(value, basestring):
      value = self._datetime_from_iso_string(value)

    super(DateTimeAttribute, self).__set__(instance, value, default=default)

  def empty(self, value):
    '''0 is not empty.'''
    return value is None


  @classmethod
  def _datetime_from_iso_string(cls, value):
    microseconds = 0
    if value and '.' in value:
      value, frag = value.rsplit('.', 1)
      frag = frag[:6]  # truncate to microseconds
      frag = frag.replace('Z', '')
      frag += (6 - len(frag)) * '0'  # add 0s
      microseconds = int(frag)

    sep = 'T' if 'T' in value else ' '
    fmt = '%Y-%m-%d' + sep + '%H:%M:%S'

    value = datetime.datetime.strptime(value, fmt)
    value = value.replace(microsecond=microseconds)
    return value




class ListAttribute(Attribute):
  '''Attribute to store lists.'''
  data_type = list
  data_value_type = str

  def __init__(self, value_type=None, **kwds):
    super(ListAttribute, self).__init__(**kwds)
    if value_type:
      self.data_value_type = value_type

  def validate(self, value):
    value = super(ListAttribute, self).validate(value)
    if value is None:
      return value

    for i in xrange(0, len(value)):
      val = value[i]
      if not isinstance(val, self.data_value_type):
        try:
          value[i] = self.data_value_type(val)
        except:
          errstr = 'internal value for attribute %s is not of type %s'
          raise TypeError(errstr % (self.name, self.data_value_type))

    return value

  def empty(self, value):
    '''[] is not empty.'''
    return value is None





class DictAttribute(ListAttribute):
  '''Attribute to store lists.'''
  data_type = dict
  data_value_type = str

  def validate(self, value):
    value = super(ListAttribute, self).validate(value)
    if value is None:
      return value

    for key, val in value.items():

      # Make sure all keys are strings
      if not isinstance(key, basestring):
        try:
          value[str(key)] = value[key]
          del value[key]
          key = str(key)
        except:
          errstr = 'internal key for attribute %s must be a string'
          raise TypeError(errstr % self.name)

      # Make sure all values are of type `data_value_type`
      if not isinstance(val, self.data_value_type):
        try:
          value[key] = self.data_value_type(val)
        except:
          errstr = 'internal value for attribute %s must be of type %s'
          raise TypeError(errstr % (self.name, self.data_value_type))

    return value

  def empty(self, value):
    '''{} is not empty.'''
    return value is None



