
import unittest
import dronestore
import bottle

from bottledronestore import Plugin

class DronestorePluginTest(unittest.TestCase):
  def setUp(self):
    self.app = bottle.Bottle(catchall=False)

  def test_with_keyword(self):
    self.repo = dronestore.Repo('/Repo')
    self.plugin = self.app.install(Plugin(self.repo))

    @self.app.get('/')
    def test(repo):
      self.assertTrue(isinstance(repo, dronestore.Repo))
      self.assertEqual(self.repo, repo)
    self.app({'PATH_INFO':'/', 'REQUEST_METHOD':'GET'}, lambda x, y: None)

  def test_with_parameter(self):
    self.repo = dronestore.Repo('/Repo')
    self.plugin = self.app.install(Plugin(self.repo))

    @self.app.get('/:param')
    def test(repo, param):
      self.assertTrue(isinstance(repo, dronestore.Repo))
      self.assertEqual(self.repo, repo)
    self.app({'PATH_INFO':'/3', 'REQUEST_METHOD':'GET'}, lambda x, y: None)

  def test_without_keyword(self):
    self.repo = dronestore.Repo('/Repo')
    self.plugin = self.app.install(Plugin(self.repo))

    @self.app.get('/')
    def test():
        pass
    self.app({'PATH_INFO':'/', 'REQUEST_METHOD':'GET'}, lambda x, y: None)

    @self.app.get('/2')
    def test(**kw):
        self.assertFalse('repo' in kw)
    self.app({'PATH_INFO':'/2', 'REQUEST_METHOD':'GET'}, lambda x, y: None)


if __name__ == '__main__':
  unittest.main()