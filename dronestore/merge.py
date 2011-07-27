
import nanotime

def merge(instance, version):
  if instance.isDirty():
    raise ValueError('Cannot merge dirty instance.')

  if not instance.isCommitted():
    raise ValueError('Cannot merge uncommitted instance.')

  mergeData = {}
  for attr in instance.attributes().values():
    rawData = attr.mergeStrategy.merge(instance.version, version)
    if rawData: # none value means no change, i.e. keep the local attribute.
      mergeData[attr.name] = rawData

  if not mergeData:
    return # nothing changed.

  # merging checks out, actually make the changes.
  for attrname, rawData in mergeData.iteritems():
    attr = instance.attributes()[attrname]
    attr.setRawData(instance, rawData)

  instance.commit()


class MergeDirection:
  '''MergeDirection represents an enumeration to identify which side to keep.'''
  Local, Remote, Merge = range(1,4)

class MergeStrategy(object):
  '''A MergeStrategy represents a unique way to decide how the two values of a
  particular attributes merge together.

  MergeStrategies are meant to enforce a particular rule that helps ensure
  application semantics regarding attributes changed in multiple nodes.

  MergeStrategies can store state in the object (e.g. a timestamp). If so,
  MergeStrategies must set the REQUIRES_STATE class variable to True.
  '''

  REQUIRES_STATE = False


  def __init__(self, attribute):
    self.attribute = attribute

  def merge(self, local_version, remote_version):
    '''merges this attribute in two different versions.'''
    raise NotImplementedError('No implementation for %s.merge()', \
      self.__class__.__name__)

  def setAttribute(self, instance, rawData, default=False):
    '''Called whenever this particular attribute is set to a new value.'''
    pass

  def _attribute_data(self, version):
    '''Returns the attribute data for this version or None'''
    try:
      attr = version.attribute(self.attribute.name)
    except KeyError:
      attr = None
    return attr


class LatestObjectStrategy(MergeStrategy):
  '''LatestObjectStrategy merges attributes based solely on objects' timestamp.
  In essence, the most recently written object wins.

  This Strategy stores no additional state.
  '''

  def merge(self, local_version, remote_version):
    if remote_version.committed > local_version.committed:
      return remote_version.attribute(self.attribute.name)
    return None





class LatestStrategy(MergeStrategy):
  '''LatestStrategy merges attributes based solely on timestamp. In essence, the
  most recently written attribute wins.

  This Strategy stores its state like so:
  { 'updated' : nanotime.nanotime, 'value': attrValue }

  A value with a timestamp will be preferred over values without.
  '''

  REQUIRES_STATE = True

  def merge(self, local_version, remote_version):

    attr_local = self._attribute_data(local_version)
    attr_remote = self._attribute_data(remote_version)

    # if no timestamp found in remote. we're done!
    if not attr_remote or 'updated' not in attr_remote:
      return None

    # since other side has a timestamp, if we don't, take theirs.
    if not attr_local or 'updated' not in attr_local:
      return attr_remote

    # if we havent decided (both have timestamps), compare timestamps
    if attr_remote['updated'] > attr_local['updated']:
      return attr_remote
    return None # no change. keep local

  def setAttribute(self, instance, rawData, default=False):
    '''Called whenever this particular attribute is set to a new value.'''
    # update the update metadata to reflect the current time.
    if default:
      rawData['updated'] = 0
    else:
      rawData['updated'] = nanotime.now().nanoseconds()





class MaxStrategy(MergeStrategy):
  '''MaxStrategy merges attributes based solely on comparison. In essence, the
  larger value is picked.

  This Strategy stores no additional state.
  '''

  def merge(self, local_version, remote_version):

    attr_local = self._attribute_data(local_version)
    attr_remote = self._attribute_data(remote_version)

    if not attr_remote:
      return None

    if not attr_local:
      return attr_remote

    if attr_remote['value'] > attr_local['value']:
      return attr_remote
    return None # no change. keep local



