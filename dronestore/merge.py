

def merge(instance, version):
  if instance.isDirty():
    raise MergeFailure('Cannot merge dirty instance.')

  merge_values = {}
  for attr in instance.attributes().values():
    value = attr.mergeStrategy.merge(instance, version)
    if value:
      merge_values[attr.name] = value

  # merging checks out, actually make the changes.
  for attrname, value in merge_values.iteritems():
    setattr(instance, attrname, value)

  instance.commit()




class MergeStrategy(object):
  '''A MergeStrategy represents a unique way to decide how the two values of a
  particular attributes merge together.

  MergeStrategies are meant to enforce a particular rule that helps ensure
  application semantics regarding attributes changed in multiple nodes.

  MergeStrategies can store state in the object (e.g. a timestamp). If so,
  MergeStrategies must set the REQUIRES_STATE class variable to True.
  '''

  REQUIRES_STATE = False

  PICK_REMOTE = 1
  PICK_LOCAL = 2
  PICK_BOTH = 3

  def __init__(self, attribute):
    self.attribute = attribute

  def merge(self, instance, version):
    raise NotImplementedError('No implementation for %s.merge()', \
      self.__class__.__name__)



class LatestObjectStrategy(MergeStrategy):
  '''LatestObjectStrategy merges attributes based solely on objects' timestamp.
  In essence, the most recently written object wins.

  This Strategy stores no additional state.
  '''

  def merge(self, instance, version):
    attr_name = self.attribute.name
    remote_value = version.attribute(attr_name)
    remote_updated = version.committed()
    local_updated = instance.version.committed()
    return remote_value if remote_updated > local_updated else None





class LatestAttributeStrategy(MergeStrategy):
  '''LatestStrategy merges attributes based solely on timestamp. In essence, the
  most recently written attribute wins.

  This Strategy stores its state like so:
  { 'updated' : nanotime.NanoTime, 'value': attrValue }

  A value with a timestamp will be preferred over values without.
  '''

  REQUIRES_STATE = True

  def merge(self, instance, version):
    attr_name = self.attribute.name
    remote_value = version.attribute(attr_name)
    mergeDirection = None

    try:
      remote_updated = other_value['updated']
    except KeyError:
      # no timestamp found in remote. we're done!
      return None

    try:
      local_updated = instance.version.attribute(attr_name)['updated']
    except KeyError:
      # other side has a timestamp, we don't. take theirs.
      mergeDirection = self.PICK_REMOTE

    if mergeDirection is None and remote_updated > local_updated:
      mergeDirection = self.PICK_REMOTE

    if mergeDirection is None:
      # picking local, that is, dont change anything. we're done!
      return None

    return remote_value
    # ok done updating. out!






class MaxStrategy(MergeStrategy):
  '''MaxStrategy merges attributes based solely on comparison. In essence, the
  larger value is picked.

  This Strategy stores no additional state.

  A value with a timestamp will be preferred over values without.
  '''

  def merge(self, instance, version):
    attr_name = self.attribute.name
    remote_value = version.attribute(attr_name)
    local_value = instance.version.attribute(attr_name)
    return remote_value if remote_value > local_value else local_value



