import time
import datetime
import unittest

from dronestore.util import nanotime

class TestTime(unittest.TestCase):

  def test_construction(self):
    eq = self.assertEqual

    # basic
    eq(nanotime.seconds(1), nanotime.nanoseconds(1e9))
    eq(nanotime.seconds(1), nanotime.microseconds(1e6))
    eq(nanotime.seconds(1), nanotime.milliseconds(1e3))
    eq(nanotime.seconds(1), nanotime.seconds(1))
    eq(nanotime.seconds(1), nanotime.minutes(1.0/60))
    eq(nanotime.seconds(1), nanotime.hours(1.0/3600))
    eq(nanotime.seconds(1), nanotime.days(1.0/(3600 * 24)))

    # timestamp
    ts1 = time.time()
    eq(nanotime.timestamp(ts1).timestamp(), ts1)

    # datetime
    dt1 = datetime.datetime.now()
    eq(nanotime.datetime(dt1).datetime(), dt1)

  def __subtest_arithmetic(self, start, extra):
    eq = self.assertEqual

    start = float(start)
    extra = float(extra)
    t1 = nanotime.seconds(start)
    eq(nanotime.seconds(start + extra), t1 + nanotime.NanoTime(extra * 1e9))
    eq(nanotime.seconds(start + extra), t1 + nanotime.seconds(extra))
    eq(nanotime.seconds(start - extra), t1 - nanotime.NanoTime(extra * 1e9))
    eq(nanotime.seconds(start - extra), t1 - nanotime.seconds(extra))
    eq(nanotime.seconds(start * extra), t1 * nanotime.NanoTime(extra))
    eq(nanotime.seconds(start * extra), t1 * nanotime.nanoseconds(extra))
    eq(nanotime.seconds(start / extra), t1 / nanotime.NanoTime(extra))
    eq(nanotime.seconds(start / extra), t1 / nanotime.nanoseconds(extra))

    self.assertTrue(nanotime.seconds(start + extra) > t1)
    self.assertTrue(nanotime.seconds(start - extra) < t1)

    t2 = nanotime.seconds(start + extra)
    self.assertTrue(t2  > nanotime.NanoTime(0))
    self.assertTrue(nanotime.NanoTime(0) < t2)

  def test_arithmetic(self):
    for start in range(0, 10000, 1000):
      for extra in range(1, 20, 5):
        self.__subtest_arithmetic(start, extra)