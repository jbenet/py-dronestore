
'''
Bottle-dronestore is a plugin that integrates dronestore with your Bottle
application. It passes a given repo to the route callback. It is heavily based
on the bottle-sqlite.

To automatically detect routes that need a repo, the plugin
searches for route callbacks that require a `repo` keyword argument
(configurable) and skips routes that do not. This removes any overhead for
routes that don't need a repo connection.

Usage Example::

    import bottle
    from dronestore.ext import dsbottle

    app = bottle.Bottle()
    plugin = sqlite.Plugin(dbfile='/tmp/test.db')
    app.install(plugin)

    @app.route('/show/:item')
    def show(item, db):
        row = db.execute('SELECT * from items where name=?', item).fetchone()
        if row:
            return template('showitem', page=row)
        return HTTPError(404, "Page not found")
'''

__author__ = 'Juan Batiz-Benet'
__email__ = 'juan@benet.ai'
__version__ = '0.1'
__license__ = 'MIT'

import inspect
try:
  from bottle import PluginError
except:
  class PluginError(Exception):
    pass


class DronestoreBottlePlugin(object):
  ''' This plugin passes a dronestore repo handle to route callbacks
      that accept a `repo` keyword argument. If a callback does not expect
      such a parameter, no repo is passed. You can override the repo
      settings on a per-route basis. '''

  name = 'dronestore'
  api = 2

  def __init__(self, repo=None, drone=None, keyword='repo'):
    repo = repo or drone # deprecate drone
    self.repo = repo
    self.keyword = keyword

  # deprecate drone
  @property
  def drone(self):
    return self.repo

  def __repr__(self):
    return '<%s %s=%s>' % \
      (DronestoreBottlePlugin, self.keyword, self.repo)

  def setup(self, app):
    ''' Make sure that other installed plugins don't affect the same
        keyword argument.'''
    for other in app.plugins:
      if not isinstance(other, DronestoreBottlePlugin):
        continue

      if other.keyword == self.keyword:
        errstr = 'dronestore bottle plugin keyword conflict between %s and %s'
        raise PluginError(errstr % (repr(self), repr(other)))

  def apply(self, callback, route):
    # Override global configuration with route-specific values.
    repo = self.repo
    if hasattr(route, 'config'):
      repo = route.config.get(self.keyword) or repo

    if not repo:
      raise PluginError('No repo specified for %s' % repr(self))

    # Test if the original callback accepts a keyword argument for this repo
    # Ignore it if it does not need a repo handle.
    if hasattr(route, 'callback'):
      args = inspect.getargspec(route.callback)[0]

      if self.keyword not in args:
        return callback

    def wrapper(*args, **kwargs):
      kwargs[self.keyword] = repo
      return callback(*args, **kwargs)

    # Replace the route callback with the wrapped one.
    return wrapper


Plugin = DronestoreBottlePlugin
