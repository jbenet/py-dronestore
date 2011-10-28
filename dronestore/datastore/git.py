
import os
import datetime
import filesystem
import subprocess

def system(cmd):
  '''Run a command on the commandline.
  use Popen. It is faster than os.system and we can monkey patch it in gevent

  '''
  subprocess.Popen(cmd, shell=True).wait()



class GitDatastore(filesystem.FSDatastore):
  '''Represents a flat-file datastore version controlled by git.'''

  def __init__(self, *args, **kwargs):
    super(GitDatastore, self).__init__(*args, **kwargs)
    self.git('init')

  def git(self, cmd):
    '''Performs git command `cmd`'''
    system('cd %s && git %s > /dev/null' % (self.directory, cmd))

  def commit(self, message):
    '''Performs a git commit'''
    self.git('commit -m "%s (git datastore)"' % message)

  def put(self, key, value):
    '''Stores the object.'''
    super(GitDatastore, self).put(key, value)
    self.git('add %s' % self.relative_path(key))
    self.commit('put %s' % key)

  def delete(self, key):
    '''Removes the object.'''
    path = self.path(key)
    if not os.path.exists(path):
      return

    self.git('rm %s' % self.relative_path(key))
    self.commit('delete %s' % key)


