
'''
Bottle-dronestore is a plugin that integrates dronestore with your Bottle
application. It passes a given drone to the route callback. It is heavily based
on the bottle-sqlite.

To automatically detect routes that need a drone, the plugin
searches for route callbacks that require a `drone` keyword argument
(configurable) and skips routes that do not. This removes any overhead for
routes that don't need a drone connection.

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
  ''' This plugin passes a dronestore drone handle to route callbacks
      that accept a `drone` keyword argument. If a callback does not expect
      such a parameter, no drone is passed. You can override the drone
      settings on a per-route basis. '''

  name = 'dronestore'
  api = 2

  def __init__(self, drone=None, keyword='drone'):
    self.drone = drone
    self.keyword = 'drone'

  def __repr__(self):
    return '<%s %s=%s>' % \
      (DronestoreBottlePlugin, self.keyword, self.drone)

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
    drone = self.drone
    if hasattr(route, 'config'):
      drone = route.config.get(self.keyword) or drone

    if not drone:
      raise PluginError('No drone specified for %s' % repr(self))

    # Test if the original callback accepts a keyword argument for this drone
    # Ignore it if it does not need a drone handle.
    if hasattr(route, 'callback'):
      args = inspect.getargspec(route.callback)[0]

      if self.keyword not in args:
        return callback

    def wrapper(*args, **kwargs):
      kwargs[self.keyword] = drone
      return callback(*args, **kwargs)

    # Replace the route callback with the wrapped one.
    return wrapper


Plugin = DronestoreBottlePlugin
