
import datetime
import hashlib
import uuid
import nanotime
import copy

from datastore import Key

from .util import serial
from .util import fasthash

import merge


class UnregisteredModelError(ValueError):
  pass

class DuplicteModelError(ValueError):
  pass

class DuplicateAttributeError(ValueError):
  pass

class InternalValueError(ValueError):
  pass



class classproperty(object):
  '''Implements both @property and @classmethod behavior.'''
  def __init__(self, getter):
    self.getter = getter
  def __get__(self, instance, owner):
    return self.getter(instance) if instance else self.getter(owner)



class Version(object):
  ''' A version is one snapshot of a particular object's values.

  Versions have an associated hash (sha1). Their hash determines uniqueness of
  the object snapshot. Versions are used as snapshot 'containers,' including
  all of the data of the particular object snapshot.

  The current implementation does not use incremental changes, as the entire
  version history of each object is not tracked.
  '''
  BLANK_HASH = '0000000000000000000000000000000000000000'
  REP_FIELDS = ['key', 'hash', 'parent', 'created', 'committed', 'attributes', \
    'type']

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
      serialRep['created'] = 0
      serialRep['committed'] = 0
      serialRep['attributes'] = {}
      serialRep['type'] = ''

    for req in self.REP_FIELDS:
      if req not in serialRep:
        raise ValueError('serial representation does not include %s' % req)

    if serialRep['created'] > serialRep['committed']:
      raise ValueError('serial representation implies created after committed')
    if serialRep['created'] < 0:
      raise ValueError('serial representation implies created before 0')

    self._serialRep = serialRep

  @property
  def key(self):
    return Key(self._serialRep['key'])

  @property
  def hash(self):
    return self._serialRep['hash']

  @property
  def type(self):
    return self._serialRep['type']

  @property
  def isBlank(self):
    return self.hash == self.BLANK_HASH

  def shortHash(self, length=6):
    return self.hash[0:length]

  @property
  def committed(self):
    return nanotime.nanotime(self._serialRep['committed'])

  @property
  def created(self):
    return nanotime.nanotime(self._serialRep['created'])

  @property
  def parent(self):
    return self._serialRep['parent']

  @property
  def serialRepresentation(self):
    return self._serialRep

  def attribute(self, name):
    try:
      return self._serialRep['attributes'][name]
    except KeyError:
      raise KeyError('No attribute %s in version' % name)

  def attributeValue(self, name):
    return self.attributeMetaData(name, 'value')

  def attributeMetaData(self, name, meta):
    attr = self.attribute(name) # outside the try to propagate up attr errors
    try: # try catch here for the usual case.
      return attr[meta]
    except KeyError:
      errstr = 'No attribute metadata \'%s\' in this version. %s'
      raise KeyError(errstr % (meta, self._serialRep.data()))

  def __getitem__(self, name):
    return self.attribute(name)

  def __eq__(self, other):
    if isinstance(other, Version):
      return self.hash == other.hash and self.key == other.key
    raise TypeError('other is not of type %s' % Version)

  def __ne__(self, other):
    return not self.__eq__(other)

  def __hash__(self):
    return fasthash.hash(self.hash)

  def __str__(self):
    return '<%s %s version %s>' % (self.type, self.key, self.hash)


attribute = None

def _initialize_attributes(cls, name, bases, attrs):
  '''This function initializes attributes (and handles name collisions).
  Attribute binding follows the model that property binding does in the Google
  App Engine.
  '''
  global attribute
  if not attribute:
    import attribute


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
    if isinstance(attr, attribute.Attribute):
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
    if type_name == 'Model' or hasattr(cls, '_unnamed_dstype'):
      cls.__dstype__ = cls.__name__
      type_name = cls.__dstype__
      cls._unnamed_dstype = True

    if type_name in REGISTERED_MODELS and REGISTERED_MODELS[type_name] != cls:
      raise DuplicteModelError('Duplicate model registered: %s' % type_name)
    REGISTERED_MODELS[type_name] = cls




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

    key = Key('/%s:%s' % (self.__dstype__, key_name))
    if parentKey:
      key = parentKey.child(key)

    for attr in self.attributes().values():
      attr.__set__(self, attr.default_value(), default=True)


    self._key = key
    self._version = Version(key)
    self._updated = None
    self._isDirty = True
    self._isPersisted = False

  def _initialize_version(self, version):
    '''Initializes from stored version data'''
    #FIXME(jbenet) consider moving this to Version class...

    if version.type != self.__class__.__dstype__:
      raise ValueError('Type name provided does not match.')

    for attr in self.attributes().values():
      try:
        value = copy.copy(version.attributeValue(attr.name))
      except KeyError:
        value = attr.default_value()
        if not value and attr.required:
          raise
      attr.__set__(self, value)


    self._key = version.key
    self._version = version
    self._isDirty = False
    self._isPersisted = True

  @classproperty
  def key(cls_or_self):
    '''The key associated with this model/instance.
    This is a classproperty in order to have:

        >>> Model.key
        Key('/Model')
        >>> Model('instance').key
        Key('/Model:instance)

    '''
    if isinstance(cls_or_self, type):
      return Key(cls_or_self.__dstype__)
    return cls_or_self._key


  @property
  def created(self):
    '''When this object was created.'''
    return self._version.created

  @property
  def committed(self):
    '''When this object was last committed.'''
    return self._version.committed

  @property
  def version(self):
    '''The current version of this object.'''
    return self._version

  def isPersisted(self):
    return self._isPersisted

  def isCommitted(self):
    return not self._version.isBlank

  def isDirty(self):
    return self._isDirty

  @classmethod
  def attributes(cls):
    '''Returns a dictionary of all the attributes defined for this model.'''
    return dict(cls._attributes)

  def attributeValues(self):
    '''Returns the attribute values of this model.'''
    return dict([(a, getattr(self, a)) for a in self.attributes()])

  def validate(self):
    '''Validates the instance attributes, ensuring invariants hold.

    Override this method (remember to call base classes' validate) to intrude
    commit-time checks that should prevent storing incorrect values.
    '''

    if self.committed < self.created:
      raise ValueError('Internal commit time is earlier than creation time')

    for attr_name, attr in self.attributes().iteritems():
      attr.validate(getattr(self, attr_name))


  def computedHash(self):
    buf = '%s,%s,' % (self._key, self.__dstype__)
    for attr_name, attr in self.attributes().iteritems():
      buf += '%s=%s,' % (attr_name, attr.rawData(self))
    return hashlib.sha1(buf).hexdigest()

  def commit(self):
    '''Committing a version creates a snapshot of the current changes.'''

    # this is actually broken for collection attributes:
    # if not self.isDirty():
    #   return # nothing to commit

    self.validate()

    sr = serial.SerialRepresentation()
    sr['hash'] = self.computedHash()
    if sr['hash'] == self._version.hash:
      self._isDirty = False
      return # false alarm, nothing to commit.

    sr['key'] = str(self.key)
    sr['type'] = self.__dstype__
    sr['parent'] = self._version.hash
    sr['created'] = self._version.created.nanoseconds()
    sr['committed'] = nanotime.now().nanoseconds()
    sr['attributes'] = {}

    if sr['created'] == 0: # from blank version
      sr['created'] = sr['committed']

    for attr_name, attr in self.attributes().iteritems():
      sr['attributes'][attr_name] = serial.clean(attr.rawData(self))

    self._version = Version(sr)

    self._isPersisted = True
    self._isDirty = False

  def merge(self, other):
    if isinstance(other, Version):
      merge.merge(self, other)
    elif isinstance(other, Model):
      merge.merge(self, other.version)
    else:
      raise TypeError('Expected instance of %s or %s' % \
        (Version, self.__class__))

  def __eq__(self, o):
    '''Check equality with another model of this kind.'''
    if not isinstance(o, self.__class__):
      return False

    if self.version != o.version:
      return False

    # if there are no changes, checking versions is enough
    if not self._isDirty and not o._isDirty:
      return True

    # we must check every attribute
    for attr in self.attributes():
      if getattr(self, attr) != getattr(o, attr):
        return False

    return True

  def __ne__(self, other):
    return not self.__eq__(other)

  @classmethod
  def modelNamed(cls, name):
    try:
      return REGISTERED_MODELS[name]
    except KeyError:
      errstr = 'Model %s is not registered. Registered models are: %s'
      raise UnregisteredModelError(errstr % (name, REGISTERED_MODELS))

  @classmethod
  def from_version(cls, version):
    return cls.modelNamed(version.type)(version)
