
import random

class RandomGen(object):
  MAX_DEPTH = 15
  DEPTH = 0

  @classmethod
  def reachedMaxDepth(cls):
    return cls.DEPTH >= cls.MAX_DEPTH

  @classmethod
  def randomString(cls):
    string = ''
    length = random.randint(0, 50)
    for i in range(0, length):
      string += chr(random.randint(ord('0'), ord('Z')))
    return string

  @classmethod
  def randomObject(cls):
    cls.DEPTH += 1

    num = random.randint(0, 6)
    if num == 0:
      return random.randint(0, 10000000)
    elif num == 1:
      return random.random()
    elif num == 2:
      return cls.randomString()
    elif num == 3:
      return cls.randomList()
    elif num == 4:
      return cls.randomDict()
    elif num == 5:
      return random.randint(0, 1) == 1

    cls.DEPTH -= 1
    return None

  @classmethod
  def randomList(cls):
    if cls.reachedMaxDepth():
      return []

    l = list()
    rng = range(0, random.randint(3, 6))
    for i in rng:
      l.append(cls.randomObject())
    return l

  @classmethod
  def randomDict(cls):
    if cls.reachedMaxDepth():
      return {}

    d = dict()
    rng = range(0, random.randint(3, 6))
    for i in rng:
      d[cls.randomString()] = cls.randomObject()
    return d
