
from datastore.query import Query as DatastoreQuery
from datastore.query import Filter, Order

from model import Key, Version, Model
from util import serial


def _object_getattr(obj, field):
  '''Aggressively tries to determine a value in `obj` for identifier `field`'''

  value = None

  # check whether this key is an attribute (Model, etc)
  if hasattr(obj, field):
    value = getattr(obj, field)

  # if not, perhaps it is an item (raw dicts, etc)
  elif field in obj:
    value = obj[field]

  # if not, perhaps it is an attributeValue (Version)
  elif hasattr(obj, 'attributeValue'):
    value = obj.attributeValue(field)

  # if not, perhaps it is an attribute (SerialRepresentations)
  elif 'attributes' in obj and field in obj['attributes']:
    value = obj['attributes'][field]['value']

  # return whatever we've got.
  return value




class Query(DatastoreQuery):

  def __init__(self, key, *args, **kwargs):

    if hasattr(key, '__dstype__'):
      key = Key(key.__dstype__)

    if isinstance(key, str):
      key = Key(key)

    super(Query, self).__init__(key, *args, **kwargs)

  def model(self):
    '''Returns the Model class associated to this query.'''
    return Model.modelNamed(self.key.name)

  object_getattr = staticmethod(_object_getattr)



def allinstances(cls, droneOrDatastore):
  '''Returns the result of querying `droneOrDatastore` with type `cls`'''
  if not issubclass(cls, Model):
    raise TypeError('cls must derive from %s' % Model)
  return droneOrDatastore.query(Query(cls))

Model.all = classmethod(allinstances)



class InstanceIterator(object):
  '''Wraps an iterator to convert Version SerialRepresentations to instances.
  Used mainly around queries to ensure iterating over the result iterator will
  return instances, not raw version data.
  '''

  def __init__(self, iterable):
    self.iter = iter(iterable)

  def __iter__(self):
    return self

  def next(self):
    '''Returns an instance of the version represented by the next object.'''
    if self.iter is None:
      raise StopIteration

    # if it returns none, return None as well. None is not necessarily the end.
    next = self.iter.next()
    if next is None:
      return None

    # if it is a dictionary, assume raw serial representation
    if isinstance(next, dict):
      next = serial.SerialRepresentation(next)

    # if it is a serialRepresentation, turn it into a Version
    if isinstance(next, serial.SerialRepresentation):
      next = Version(next)

    # if it is a Version, turn it into a Model
    if isinstance(next, Version):
      next = Model.from_version(next)

    # return whatever it is we have!
    return next


