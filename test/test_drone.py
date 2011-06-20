
import os
import sys
import random
import unittest

from dronestore.datastore.lrucache import LRUCache
from dronestore import Key, Model, Drone

from test_merge import PersonM


class TestDrone(unittest.TestCase):

  def test_simple(self):
    drone = Drone('/DroneA/', LRUCache(100))

    p = PersonM('A')
    p.first = 'A'
    p.last = 'B'
    p.commit()

    self.assertEqual(drone.get(p.key), None)

    drone.put(p)
    self.assertEqual(drone.get(p.key), p)

    for i in range(0, 100):
      drone.delete(p.key)
      self.assertEqual(drone.get(p.key), None)

      drone.put(p)
      self.assertEqual(drone.get(p.key), p)


    p2 = PersonM(p.version)
    self.assertEqual(p2, drone.get(p2.key))

    p2.first = 'B'
    p2.commit()

    self.assertNotEqual(p, p2)
    self.assertNotEqual(p.first, p2.first)
    self.assertNotEqual(p2, drone.get(p2.key))

    p2 = drone.merge(p2)

    self.assertEqual(p2, drone.get(p2.key))


  def test_stress(self):
    num_drones = 5
    num_people = 10

    drones = []
    for i in range(0, num_drones):
      drones.append(Drone('/Drone%s' % i, LRUCache(num_people)))
      print 'Created drone ', drones[-1].droneid


    people = []
    for i in range(0, num_people):
      p = PersonM('person%s' % i)
      p.first = 'first%d:' % i
      p.last = 'last%d:' % i
      p.phone = '%d:' % i
      p.gender = '%d:' % i
      p.age = 0
      p.commit()

      d = random.choice(drones)
      d.put(p)
      print 'Created person: ', p.key

    def randomPerson(drone):
      i = random.randint(0, num_people - 1)
      return drone.get(Key('/PersonM/person%s' % i))

    def updateField(field):
      d = random.choice(drones)
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
      d1, d2 = random.sample(drones, 2)
      p = randomPerson(d1)
      if p is None:
        return

      try:
        d2.merge(p)
      except KeyError, e:
        d2.put(p.version)


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

    for d in drones:
      print 'Drone: ', d.droneid
      for i in range(num_people):
        key = Key('/PersonM/person%s' % i)
        print key,

        p = d.get(key)
        if p is None:
          print 'not found'
        else:
          print p

    for i in range(num_people):
      key = Key('/PersonM/person%s' % i)
      p = drones[0].get(key)
      for d in drones:
        p = d.merge(p)

      # doule pass.
      for d in drones:
        p = d.merge(p)
        o = d.get(p.key)
        self.assertEqual(p, o)
        self.assertEqual(p.first, o.first)
        self.assertEqual(p.last, o.last)
        self.assertEqual(p.phone, o.phone)
        self.assertEqual(p.age, o.age)
        self.assertEqual(p.gender, o.gender)
        self.assertEqual(p.version, o.version)
