import time
import datetime

datetime_module = datetime


'''
This module provides a time object that keeps time as the number of nanoseconds
since the UNIX epoch. In other words, it is a UNIX timestamp with nanosecond
precision.

As a c-struct, it is a 64bit integer (for network use).
'''

class NanoTime(object):

  def __init__(self, nanoseconds):
    if isinstance(nanoseconds, self.__class__):
      self._ns = nanoseconds._ns
    else:
      self._ns = int(round(nanoseconds))

  #----------------------------------------------------
  def days(self):
    return self._ns / (1.0e9 * 60 * 60 * 24)

  def hours(self):
    return self._ns / (1.0e9 * 60 * 60)

  def minutes(self):
    return self._ns / (1.0e9 * 60)

  def seconds(self):
    return self._ns / 1.0e9

  def milliseconds(self):
    return self._ns / 1.0e6

  def microseconds(self):
    return self._ns / 1.0e3

  def nanoseconds(self):
    return self._ns

  #----------------------------------------------------

  def unixtime(self):
    return self.seconds()

  def timestamp(self):
    return self.seconds()

  def datetime(self):
    print 'ns:', self._ns
    print 'ts:', self.timestamp()
    return datetime_module.datetime.fromtimestamp(self.timestamp())

  #----------------------------------------------------

  def __str__(self):
    return str(self.datetime())

  #----------------------------------------------------

  def __add__(self, other):
    return NanoTime(self._ns + other._ns)

  def __sub__(self, other):
    return NanoTime(self._ns - other._ns)

  def __mul__(self, other):
    return NanoTime(self._ns * other._ns)

  def __div__(self, other):
    return NanoTime(self._ns * 1.0 / other._ns)

  def __cmp__(self, other):
    return cmp(self._ns, other._ns)

  def __hash__(self):
    return hash(self._ns)


class NanoTimeFactory(object):

  @classmethod
  def days(cls, m):
    return NanoTime(m * 1.0e9 * 60 * 60 * 24)

  @classmethod
  def hours(cls, m):
    return NanoTime(m * 1.0e9 * 60 * 60)

  @classmethod
  def minutes(cls, m):
    return NanoTime(m * 1.0e9 * 60)

  @classmethod
  def seconds(cls, s):
    return NanoTime(s * 1.0e9)

  @classmethod
  def milliseconds(cls, ms):
    return NanoTime(ms * 1.0e6)

  @classmethod
  def microseconds(cls, us):
    return NanoTime(us * 1.0e3)

  @classmethod
  def nanoseconds(cls, ns):
    return NanoTime(ns)

  #----------------------------------------------------

  @classmethod
  def unixtime(cls, unixtime):
    return cls.seconds(unixtime)

  @classmethod
  def timestamp(cls, ts):
    return cls.seconds(ts)

  @classmethod
  def datetime(cls, d):
    return cls.microseconds(time.mktime(d.timetuple())*1e6 + d.microsecond)

  #----------------------------------------------------

  @classmethod
  def now(cls):
    return cls.seconds(time.time())


days = NanoTimeFactory.days
hours = NanoTimeFactory.hours
minutes = NanoTimeFactory.minutes
seconds = NanoTimeFactory.seconds
milliseconds = NanoTimeFactory.milliseconds
microseconds = NanoTimeFactory.microseconds
nanoseconds = NanoTimeFactory.nanoseconds
unixtime = NanoTimeFactory.unixtime
timestamp = NanoTimeFactory.timestamp
datetime = NanoTimeFactory.datetime
now = NanoTimeFactory.now


