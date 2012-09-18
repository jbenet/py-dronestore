
import os
import sys
import random
import unittest

from datastore.impl.lrucache import LRUCache
from dronestore import Key, Model, Repo, Query

from test_merge import PersonM


class TestRepo(unittest.TestCase):

  def test_simple(self):
    repo = Repo('/RepoA/', LRUCache(100))

    p = PersonM('A')
    p.first = 'A'
    p.last = 'B'
    p.commit()

    self.assertFalse(repo.contains(p.key))
    self.assertEqual(repo.get(p.key), None)

    repo.put(p)
    self.assertTrue(repo.contains(p.key))
    self.assertEqual(repo.get(p.key), p)
    for i in range(0, 100):
      repo.delete(p.key)
      self.assertEqual(repo.get(p.key), None)
      self.assertFalse(repo.contains(p.key))

      repo.put(p)
      self.assertEqual(repo.get(p.key), p)
      self.assertTrue(repo.contains(p.key))


    p2 = PersonM(p.version)
    self.assertEqual(p2, repo.get(p2.key))

    p2.first = 'B'
    p2.commit()

    self.assertNotEqual(p, p2)
    self.assertNotEqual(p.first, p2.first)
    self.assertNotEqual(p2, repo.get(p2.key))

    p2 = repo.merge(p2)

    self.assertEqual(p2, repo.get(p2.key))

    q = Query(PersonM).filter('first', '=', 'B')
    res = list(repo.query(q))

    self.assertTrue(len(res) == 1)
    self.assertEqual(p2, res[0])


  def test_stress(self):
    num_repos = 5
    num_people = 10

    repos = []
    for i in range(0, num_repos):
      repos.append(Repo('/Repo%s' % i, LRUCache(num_people)))
      print 'Created repo ', repos[-1].repoid


    people = []
    for i in range(0, num_people):
      p = PersonM('person%s' % i)
      p.first = 'first%d:' % i
      p.last = 'last%d:' % i
      p.phone = '%d:' % i
      p.gender = '%d:' % i
      p.age = 0
      p.commit()

      d = random.choice(repos)
      d.put(p)
      print 'Created person: ', p.key

    def randomPerson(repo):
      i = random.randint(0, num_people - 1)
      return repo.get(Key('/PersonM:person%s' % i))

    def updateField(field):
      d = random.choice(repos)
      p = randomPerson(d)
      if p is None:
        return

      if field == 'age':
        p.age += 1
      else:
        setattr(p, field, field + str(i))
      p.commit()
      d.merge(p)

    def shuffle():
      d1, d2 = random.sample(repos, 2)
      p = randomPerson(d1)
      if p is None:
        return

      d2.merge(p)


    for i in range(num_people * 10):
      updateField('first')
      updateField('last')
      updateField('phone')
      updateField('age')
      updateField('gender')

      shuffle()
      shuffle()
      shuffle()
      shuffle()
      shuffle()

    for d in repos:
      print 'Repo: ', d.repoid
      for i in range(num_people):
        key = Key('/PersonM:person%s' % i)

        p = d.get(key)
        if p is None:
          print key, 'not found'
        else:
          print p

    for i in range(num_people):
      key = Key('/PersonM:person%s' % i)
      p = repos[0].get(key)
      for d in repos:
        p = d.merge(p)

      # doule pass.
      for d in repos:
        p = d.merge(p)
        o = d.get(p.key)
        self.assertEqual(p, o)
        self.assertEqual(p.first, o.first)
        self.assertEqual(p.last, o.last)
        self.assertEqual(p.phone, o.phone)
        self.assertEqual(p.age, o.age)
        self.assertEqual(p.gender, o.gender)
        self.assertEqual(p.version, o.version)

        print 'In order:'
        last = None
        for person in d.query(Query(PersonM).order('key')):
          if last:
            self.assertTrue(str(person.key) > last)
          last = str(person.key)
          print person

        print 'Reverse:'
        last = None
        for person in d.query(Query(PersonM).order('-key')):
          if last:
            self.assertTrue(str(person.key) < last)
          last = str(person.key)
          print person

        result = d.query(Query(PersonM, limit=3).order('-key'))
        self.assertTrue(len(list(result)) == 3)

if __name__ == '__main__':
  unittest.main()
