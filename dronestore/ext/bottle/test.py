
import unittest
import dronestore
import bottle

from bottledronestore import Plugin

class DronestorePluginTest(unittest.TestCase):
  def setUp(self):
    self.app = bottle.Bottle(catchall=False)

  def test_with_keyword(self):
    self.drone = dronestore.Drone('/Drone')
    self.plugin = self.app.install(Plugin(self.drone))

    @self.app.get('/')
    def test(drone):
      self.assertTrue(isinstance(drone, dronestore.Drone))
      self.assertEqual(self.drone, drone)
    self.app({'PATH_INFO':'/', 'REQUEST_METHOD':'GET'}, lambda x, y: None)

  def test_with_parameter(self):
    self.drone = dronestore.Drone('/Drone')
    self.plugin = self.app.install(Plugin(self.drone))

    @self.app.get('/:param')
    def test(drone, param):
      self.assertTrue(isinstance(drone, dronestore.Drone))
      self.assertEqual(self.drone, drone)
    self.app({'PATH_INFO':'/3', 'REQUEST_METHOD':'GET'}, lambda x, y: None)

  def test_without_keyword(self):
    self.drone = dronestore.Drone('/Drone')
    self.plugin = self.app.install(Plugin(self.drone))

    @self.app.get('/')
    def test():
        pass
    self.app({'PATH_INFO':'/', 'REQUEST_METHOD':'GET'}, lambda x, y: None)

    @self.app.get('/2')
    def test(**kw):
        self.assertFalse('drone' in kw)
    self.app({'PATH_INFO':'/2', 'REQUEST_METHOD':'GET'}, lambda x, y: None)


if __name__ == '__main__':
  unittest.main()