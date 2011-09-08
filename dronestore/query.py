
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




def allinstances(cls, droneOrDatastore):
  '''Returns the result of querying `droneOrDatastore` with type `cls`'''
  if not issubclass(cls, Model):
    raise TypeError('cls must derive from %s' % Model)
  return droneOrDatastore.query(Query(cls))

Model.all = classmethod(allinstances)





class Filter(object):
  '''Represents a Filter for a specific field and its value.
  Filters are used on queries to narrow down the set of matching objects.
  '''

  CONDITIONAL_OPERATORS = {
    "<"  : lambda a, b: a < b,
    "<=" : lambda a, b: a <= b,
    "="  : lambda a, b: a == b,
    "!=" : lambda a, b: a != b,
    ">=" : lambda a, b: a >= b,
    ">"  : lambda a, b: a > b
  }

  def __init__(self, field, op, value):
    if op not in self.CONDITIONAL_OPERATORS:
      raise ValueError('"%s" is not a valid filter Conditional Operator' % op)

    self.field = field
    self.op = op
    self.value = value

  def __call__(self, obj):
    '''Returns whether this version passes this filter.
    This method aggressively tries to find the appropriate value.
    '''

    value = _object_getattr(obj, self.field)

    if isinstance(self.value, str) and not isinstance(value, str):
      value = str(value)
    return self.valuePasses(value)

  def valuePasses(self, value):
    '''Returns whether this value passes this filter'''
    return self.CONDITIONAL_OPERATORS[self.op](value, self.value)


  def __str__(self):
    return '%s %s %s' % (self.field, self.op, self.value)

  def __repr__(self):
    return "Filter('%s', '%s', %s)" % (self.field, self.op, repr(self.value))


  def __eq__(self, o):
    return self.field == o.field and self.op == o.op and self.value == o.value

  def __ne__(self, other):
    return not self.__eq__(other)

  def __hash__(self):
    return hash(repr(self))


  @classmethod
  def metaFilter(cls, filters):
    '''Returns a function to filter an item with given `filters`'''
    return lambda item: all([f(item) for f in filters])

  @classmethod
  def filter(cls, filters, items):
    '''Returns the elements in `items` that pass given `filters`'''
    return filter(cls.metaFilter(filters), items)





class Order(object):
  '''Represents an Order upon a specific field, and a direction.
  Orders are used on queries to define how they operate on objects
  '''

  ORDER_OPERATORS = ['-', '+']

  def __init__(self, order):
    self.op = '+'

    try:
      if order[0] in self.ORDER_OPERATORS:
        self.op = order[0]
        order = order[1:]
    except IndexError:
      raise ValueError('Order input be at least two characters long.')

    self.field = order

    if self.op not in self.ORDER_OPERATORS:
      raise ValueError('"%s" is not a valid Order Operator.' % op)


  def __str__(self):
    return '%s%s' % (self.op, self.field)

  def __repr__(self):
    return "Order('%s%s')" % (self.op, self.field)


  def __eq__(self, other):
    return self.field == other.field and self.op == other.op

  def __ne__(self, other):
    return not self.__eq__(other)

  def __hash__(self):
    return hash(repr(self))


  def isAscending(self):
    return self.op == '+'

  def isDescending(self):
    return not self.isAscending()


  def keyfn(self, obj):
    '''A key function to be used in pythonic sort operations.'''
    return _object_getattr(obj, self.field)

  @classmethod
  def metaOrder(cls, orders):
    '''Returns a function that will compare two items according to `orders`'''
    comparers = [ (o.keyfn, 1 if o.isAscending() else -1) for o in orders]

    def cmpfn(a, b):
      for keyfn, ascOrDesc in comparers:
        comparison = cmp(keyfn(a), keyfn(b)) * ascOrDesc
        if comparison is not 0:
          return comparison
      return 0

    return cmpfn

  @classmethod
  def sorted(cls, items, orders):
    '''Returns the elements in `items` sorted according to `orders`'''
    return sorted(items, cmp=cls.metaOrder(orders))





class Query(object):
  '''A Query describes a set versions.

  Queries are used to retrieve versions and instances matching a set of criteria
  from Datastores and Drones. Query objects themselves are simply descriptions,
  the actual implementations are left up to the Datastores.
  '''

  DEFAULT_LIMIT = 2000

  def __init__(self, dstype, limit=None, offset=0, keysonly=False):
    self.type = dstype if isinstance(dstype, basestring) else dstype.__dstype__

    self.limit = int(limit) if limit is not None else self.DEFAULT_LIMIT
    self.offset = int(offset)
    self.keysonly = bool(keysonly)

    self.filters = []
    self.orders = []

  def model(self):
    '''Returns the Model class associated to this query.'''
    return Model.modelNamed(self.type)

  def __str__(self):
    '''Returns a string describing this query.'''
    return '<%s for %s>' % (self.__class__, self.type)

  def __repr__(self):
    '''Returns the representation of this query. Enables eval(repr(.)).'''
    return 'Query.from_dict(%s)' % self.dict()

  def __call__(self, sequence):
    '''Naively apply this query on a sequence of objects.
    Applying a query converts versions to instances, applies filters, sorts
    by the appropriate orders, and returns a limited set.

    WARNING: due to the need to order the results, this function operates on
             entire entity sequences directly, not just iterators. That means
             the entire result set will be in memory. Datastores with large
             objects and large query results should translate the Query and
             perform their own optimizations.
    '''
    sequence = filter(self.filterFn, sequence)
    sequence = sorted(sequence, cmp=self.orderFn)
    return sequence[self.offset:self.limit]

  def order(self, order):
    '''Adds an Order to this query.

    Returns self for JS-like method chaining:
    query.filter('age', '>', 18).filter('home', '=', 'San Francisco')
    '''

    order = order if isinstance(order, Order) else Order(order)
    self.orders.append(order)
    return self

  @property
  def orderFn(self):
    '''Returns a function that orders items with the query's orders'''
    return Order.metaOrder(self.orders)


  def filter(self, *args):
    '''Adds a Filter to this query.

    Returns self for JS-like method chaining:
    query.filter('age', '>', 18).filter('sex', '=', 'Female')
    '''
    if len(args) == 1 and isinstance(args[0], Filter):
      filter = args[0]
    else:
      filter = Filter(*args)
    self.filters.append(filter)
    return self # for JSlike chaining: q.filter('age', '>', 18).filter(...)

  @property
  def filterFn(self):
    '''Returns a function that filters an item with the query's filters'''
    return Filter.metaFilter(self.filters)


  def __cmp__(self, other):
    return cmp(self.dict(), other.dict())

  def __hash__(self):
    return hash(repr(self))


  def dict(self):
    '''Returns a dictionary representing this query.'''

    d = dict( { 'type' : self.type } )

    if self.limit != self.DEFAULT_LIMIT:
      d['limit'] = self.limit
    if self.offset > 0:
      d['offset'] = self.offset
    if len(self.filters) > 0:
      d['filter'] = [[f.field, f.op, f.value] for f in self.filters]
    if len(self.orders) > 0:
      d['order'] = [str(o) for o in self.orders]
    if self.keysonly:
      d['keysonly'] = self.keysonly

    return serial.clean(d)

  @classmethod
  def from_dict(cls, dictionary):
    '''Constructs a query from a dictionary.'''
    query = cls(dictionary['type'])
    for key, value in dictionary.items():

      if key == 'order':
        for order in value:
          query.order(order)

      elif key == 'filter':
        for filter in value:
          if not isinstance(filter, Filter):
            filter = Filter(*filter)
          query.filter(filter)

      elif key in ['limit', 'offset', 'keysonly']:
        setattr(query, key, value)
    return query





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


