
import os
import json
import basic

class FSDatastore(basic.Datastore):
  '''Represents a flat-file datastore.'''

  def __init__(self, directory, serializer=json):
    if len(directory) < 1:
      raise ValueError('directory must be a sring of at least length 1')
    self.directory = os.path.abspath(directory)
    self.ensure_directory_exists(self.directory)

    errstr = 'serializer must provide inverse functions `loads` and `dumps`'
    assert( {} == serializer.loads(serializer.dumps({})), errstr)
    self.serializer = serializer

  def relative_path(self, key):
    '''Returns the `relative_path` for given `key`'''
    return str(key)[1:] # remove first slash (absolute)


  def path(self, key):
    '''Returns the `path` for given `key`'''
    return os.path.join(self.directory, self.relative_path(key))


  def read_object_from_file(self, path):
    '''read in object from file at `path`'''
    # if the file doesn't exist, return None
    if not os.path.isfile(path):
      return None

    # get file contents
    with open(path) as f:
      file_contents = f.read()

    return self.serializer.loads(file_contents)

  def write_object_to_file(self, path, value):
    '''write out `object` to file at `path`'''
    self.ensure_directory_exists(os.path.dirname(path))

    with open(path, 'w') as f:
      f.write(self.serializer.dumps(value))

  @staticmethod
  def ensure_directory_exists(directory):
    '''ensure `directory` exists. if it does not, `mkdir -p directory`'''
    if not os.path.isdir(directory):
      os.makedirs(directory)


  def get(self, key):
    '''Return the object named by key.'''
    return self.read_object_from_file(self.path(key))

  def put(self, key, value):
    '''Stores the object.'''
    self.write_object_to_file(self.path(key), value)

  def delete(self, key):
    '''Removes the object.'''
    path = self.path(key)
    if not os.path.exists(path):
      return

    os.remove(path)

  def contains(self, key):
    '''Returns whether the object is in this datastore.'''
    path = self.path(key)
    return os.path.exists(path) and os.path.isfile(path)

  def query(self, query):
    '''Returns a sequence of objects matching criteria expressed in `query`'''
    path = os.path.join(self.directory, query.type)
    if not os.path.exists(path):
      return []

    filenames = os.listdir(path)
    paths = map(lambda f: os.path.join(path, f), filenames)
    objects = map(lambda f: self.read_object_from_file(f), paths)
    return query(objects)





class prettyjson(object):
  '''json wrapper that pretty-prints. useful for reading or versioning'''

  @classmethod
  def loads(cls, data):
    return json.loads(data)

  @classmethod
  def dumps(cls, data):
    return json.dumps(data, sort_keys=True, indent=1)
