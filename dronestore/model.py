
import datetime
import hashlib
import uuid

from util import nanotime
from util import serial
from util import fasthash

import merge

class DuplicteModelError(ValueError):
  pass

class Key(object):
  '''A key represents the unique identifier of an object.
  Our key scheme is inspired by the Google App Engine key model.

  It is meant to be unique across a system. Note that keys are hierarchical,
  objects can be deemed the 'children' of other objects. It is also strongly
  encouraged to include the 'type' of the object in the key path.

  For example:
    Key('/ComedyGroups/MontyPython')
    Key('/ComedyGroups/MontyPython/Comedian/JohnCleese')
  '''
  def __init__(self, key):
    self._str = self.removeDuplicateSlashes(str(key))

  def name(self):
    return self._str.rsplit('/', 1)[-1]

  def parent(self):
    if '/' in self._str:
      return Key(self._str.rsplit('/', 1)[0])
    raise ValueError('Key %s is base key (i.e. it has no parent)' % self)

  def child(self, other):
    return Key('%s/%s' % (self._str, str(other)))

  def isAncestorOf(self, other):
    if isinstance(other, Key):
      return other._str.startswith(self._str)
    raise TypeError('other is not of type %s' % Key)

  def isTopLevel(self):
    return self._str.rfind('/') == 0

  def __hash__(self):
    return fasthash.hash(self)

  def __str__(self):
    return self._str

  def __repr__(self):
    return self._str

  def __iter__(self):
    return iter(self._str)

  def __len__(self):
    return len(self._str)

  def __cmp__(self, other):
    if isinstance(other, Key):
      return cmp(self._str, other._str)
    raise TypeError('other is not of type %s' % Key)

  @classmethod
  def randomKey(cls):
    return Key(uuid.uuid4().hex)

  @classmethod
  def removeDuplicateSlashes(cls, path):
    return '/'.join([''] + filter(lambda p: p != '', path.split('/')))





class Version(object):
  ''' A version is one snapshot of a particular object's values.

  Versions have an associated hash (sha1). Their hash determines uniqueness of
  the object snapshot. Versions are used as snapshot 'containers,' including
  all of the data of the particular object snapshot.

  The current implementation does not use incremental changes, as the entire
  version history of each object is not tracked.
  '''
  BLANK_HASH = '0000000000000000000000000000000000000000'

  def __init__(self, keyOrRepresentation):
    serialRep = None
    key = None

    if isinstance(keyOrRepresentation, serial.SerialRepresentation):
      serialRep = keyOrRepresentation
    elif isinstance(keyOrRepresentation, Key):
      key = keyOrRepresentation
    else:
      raise ValueError('No serial representation or key provided.')


    if serialRep is None:
      serialRep = serial.SerialRepresentation()
      serialRep['key'] = str(key)
      serialRep['hash'] = self.BLANK_HASH
      serialRep['parent'] = self.BLANK_HASH
      serialRep['committed'] = 0
      serialRep['attributes'] = {}
      serialRep['type'] = ''

    required = ['key', 'hash', 'parent', 'committed', 'attributes', 'type']
    for req in required:
      if req not in serialRep:
        raise ValueError('serial representation does not include %s' % req)

    self._serialRep = serialRep

  def key(self):
    return Key(self._serialRep['key'])

  def hash(self):
    return self._serialRep['hash']

  def type(self):
    return self._serialRep['type']

  def isBlank(self):
    return self.hash() == self.BLANK_HASH

  def shortHash(self, length=6):
    return self.hash()[0:length]

  def committed(self):
    return nanotime.NanoTime(self._serialRep['committed'])

  def parent(self):
    return self._serialRep['parent']

  def serialRepresentation(self):
    return self._serialRep

  def attribute(self, name):
    try:
      if name in self._serialRep['attributes']:
        return self._serialRep['attributes'][name]
      raise KeyError('No attribute %s in this version' % name)
    except KeyError:
      raise KeyError('No attributes in this version. SerialRep corrupted.')

  def attributeValue(self, name):
    return self.attributeMetaData(name, 'value')

  def attributeMetaData(self, name, meta):
    attr = self.attribute(name) # outside the try to propagate up attr errors
    try: # try catch here for the usual case.
      return attr[meta]
    except KeyError:
      raise KeyError('No attribute metadata %s in this version' % meta)

  def __getitem__(self, name):
    return self.attribute(name)

  def __eq__(self, other):
    if isinstance(other, Version):
      return self.hash() == other.hash() and self.key() == other.key()
    raise TypeError('other is not of type %s' % Version)

  def __hash__(self):
    return hash(self.hash())

  def __contains__(self, other):
    return other in self._str


def _initialize_attributes(cls, name, bases, attrs):
  '''This function initializes attributes (and handles name collisions).
  Attribute binding follows the model that property binding does in the Google
  App Engine.
  '''

  cls._attributes = {}
  defined_attrs = {}

  # this walks the bases to find which class added the given attr.
  def get_attr_source(cls, attr):
    for src_cls  in cls.mro():
      if attr in src_cls.__dict__:
        return src_cls

  # Gather all the ds attributes from all the bases.
  for base in bases:
    if hasattr(base, '_attributes'):
      keys = set(base._attributes.keys())
      dupe_keys = set(defined_attrs.keys()) & keys
      for dupe_key in dupe_keys:
        old_source = defined_attrs[dupe_key]
        new_source = get_attr_source(base, dupe_prop_name)
        if old_source != new_source:
          raise DuplicateAttributeError(
              'Duplicate attribute, %s, is inherited from both %s and %s.' %
              (dupe_prop_name, old_source.__name__, new_source.__name__))

      keys -= dupe_keys
      if keys:
        defined_attrs.update(dict.fromkeys(keys, base))
        cls._attributes.update(base._attributes)

  # add the ds attributes from this class.
  for attr_name in attrs.keys():
    attr = attrs[attr_name]
    if isinstance(attr, Attribute):
      # reserved word check here.
      if attr_name in defined_attrs:
        raise DuplicateAttributeError('Duplicate attribute: %s' % attr_name)
      defined_attrs[attr_name] = cls
      cls._attributes[attr_name] = attr
      attr._attr_config(cls, attr_name)




REGISTERED_MODELS = {}

class ModelMeta(type):
  '''This is the meta class for Model.
  It sets up model attributes and registers the object.
  '''
  def __init__(cls, name, bases, attrs):
    super(ModelMeta, cls).__init__(name, bases, attrs)

    _initialize_attributes(cls, name, bases, attrs)

    type_name = cls.__dstype__
    if type_name == 'Model':
      cls.__dstype__ = cls.__name__
      type_name = cls.__dstype__

#FIXME(jbenet)
#    if type_name in REGISTERED_MODELS:
#      raise DuplicteModelError('Duplicate model registered: %s' % type_name)
    REGISTERED_MODELS[type_name] = cls




class Attribute(object):
  '''Attributes define and compose a Model. A Model can be seen as a collection
  of attributes.

  An Attribute primarily defines a name, an associated data type, and a
  particular merge strategy.

  Attributes can have other options, including defining a default value, and
  validation for the data they hold.
  '''
  data_type = str

  def __init__(self, name=None, default=None, required=False, strategy=None):

    if not strategy:
      strategy = merge.LatestObjectStrategy
    strategy = strategy(self)

    if not isinstance(strategy, merge.MergeStrategy):
      raise TypeError('mergeStrategy does not inherit from %s' % \
        merge.MergeStrategy.__name__)

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
      return getattr(instance, self._attr_name())['value']
    except AttributeError:
      return None

  def __set__(self, instance, value):
    '''Validate and Set the attribute on the model instance.'''
    value = self.validate(value)

    rawData = self.rawData(instance)
    if rawData is None:
      rawData = {}
      setattr(instance, self._attr_name(), rawData)

    # our attributes are idempotent, so if its the same, doesn't change state
    if 'value' in rawData and rawData['value'] == value:
      return

    rawData['value'] = value
    instance._isDirty = True
    self.mergeStrategy.setAttribute(instance, rawData)

  def default_value(self):
    '''The default value for a particular attribute.'''
    return self.default

  def validate(self, value):
    '''Assert that the provided value is compatible with this attribute.'''
    if self.empty(value):
      if self.required:
        raise ValueError('Attribute %s is required.' % self.name)

    if value is not None and not isinstance(value, self.data_type):
      value = self.data_type(value)

    return value

  def empty(self, value):
    '''Simple check to determine if value is empty.'''
    return not value





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
  data_type = Key

  def __init__(self, **kwds):
    super(KeyAttribute, self).__init__(multiline=False, **kwds)


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



class TimeAttribute(Attribute):
  '''Attribute to store nanosecond times.'''
  data_type = nanotime.NanoTime





class Model(object):
  '''Model'''
  __metaclass__ = ModelMeta
  __dstype__ = 'Model'

  def __init__(self, keyNameOrVersion, parentKey=None):
    '''Initializes the model by reconstructing from version or blank state.'''

    if isinstance(keyNameOrVersion, Version):
      self._initialize_version(keyNameOrVersion)

    elif isinstance(keyNameOrVersion, Key) or isinstance(keyNameOrVersion, str):
      self._initialize_new(keyNameOrVersion, parentKey)

    else:
      raise ValueError('No key or version provided.')


  def _initialize_new(self, key_name, parentKey):
    '''Initializes with blank state (only key)'''

    key_name = str(key_name)
    if '/' in key_name:
      raise ValueError('Key name %s includes slashes. It must not.' % key_name)

    key = Key('/%s/%s' % (self.__dstype__, key_name))
    if parentKey:
      key = parentKey.child(key)

    for attr in self.attributes().values():
      attr.__set__(self, attr.default_value())

    self._key = key
    self._version = Version(key)
    self._created = None
    self._updated = None
    self._isDirty = True
    self._isPersisted = False

  def _initialize_version(self, version):
    '''Initializes from stored version data'''
    #FIXME(jbenet) consider moving this to Version class...

    if version.type() != self.__class__.__dstype__:
      raise ValueError('Type name provided does not match.')

    for attr in self.attributes().values():
      attr.__set__(self, version.attributeValue(attr.name))

    self._key = version.key()
    self._version = version
    self._created = None # Fixme(jbenet)
    self._updated = None # Fixme(jbenet)
    self._isDirty = False
    self._isPersisted = True


  @property
  def key(self):
    '''The key associated with this model.'''
    return self._key

  @property
  def created(self):
    '''When this object was created.'''
    return self._created

  @property
  def updated(self):
    '''When this object was updated.'''
    return self._updated

  @property
  def version(self):
    '''The current version of this object.'''
    return self._version

  def isPersisted(self):
    return self._isPersisted

  def isCommitted(self):
    return not self._version.isBlank()

  def isDirty(self):
    return self._isDirty

  @classmethod
  def attributes(cls):
    '''Returns a dictionary of all the attributes defined for this model.'''
    return dict(cls._attributes)

  def computedHash(self):
    buf = '%s,%s,' % (self._key, self.__dstype__)
    for attr_name, attr in self.attributes().iteritems():
      buf += '%s=%s,' % (attr_name, getattr(self, attr_name))
    return hashlib.sha1(buf).hexdigest()

  def commit(self):
    '''Committing a version creates a snapshot of the current changes.'''

    if not self.isDirty():
      return # nothing to commit

    sr = serial.SerialRepresentation()
    sr['hash'] = self.computedHash()
    if sr['hash'] == self._version.hash():
      self._isDirty = False
      return # false alarm, nothing to commit.

    sr['key'] = str(self.key)
    sr['type'] = self.__dstype__
    sr['parent'] = self._version.hash()
    sr['committed'] = nanotime.now().nanoseconds()
    sr['attributes'] = {}

    for attr_name, attr in self.attributes().iteritems():
      sr['attributes'][attr_name] = attr.rawData(self) # merge here??

    self._version = Version(sr)

    self._isPersisted = True
    self._isDirty = False

  def merge(self, other):
    if isinstance(other, Version):
      merge.merge(self, version)
    elif isinstance(other, Model):
      merge.merge(self, other.version)
    else:
      raise TypeError('Merge must be an instance of either Version or Model')

  def __eq__(self, o):
    if isinstance(o, Model):

      # if there are changes, we must check every attribute
      if self._isDirty or o._isDirty:
        for attr in self.attributes():
          if getattr(self, attr) != getattr(o, attr):
            return False

      # versions must match
      return self.version == o.version

    return False

  @classmethod
  def modelNamed(cls, name):
    return REGISTERED_MODELS[name]

  @classmethod
  def from_version(cls, version):
    return cls.modelNamed(version.type())(version)

