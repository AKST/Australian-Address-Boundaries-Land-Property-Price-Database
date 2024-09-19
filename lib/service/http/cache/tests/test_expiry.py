from datetime import datetime, timedelta
import unittest

from lib.service.http.cache.expiry import *

class CacheExpireTestCase(unittest.TestCase):
    def test_parse_never(self):
        parsed = CacheExpire.parse_expire('never')
        self.assertEqual(parsed, Never())
        self.assertEqual('never', str(Never()))

    def test_parse_delta(self):
        parsed = CacheExpire.parse_expire('delta:days:5')
        self.assertEqual(parsed, Delta('days', 5))
        self.assertEqual('delta:days:5', str(Delta('days', 5)))

        parsed = CacheExpire.parse_expire('delta:weeks:2')
        self.assertEqual(parsed, Delta('weeks', 2))
        self.assertEqual('delta:weeks:2', str(Delta('weeks', 2)))

    def test_parse_till_next_day_of_week(self):
        parsed = CacheExpire.parse_expire('till_next_day_of_week:Monday')
        day_ow = TillNextDayOfWeek(0)
        self.assertEqual(parsed, day_ow)
        self.assertEqual('till_next_day_of_week:Monday', str(day_ow))

        parsed = CacheExpire.parse_expire('till_next_day_of_week:Wednesday')
        day_ow = TillNextDayOfWeek(2)
        self.assertEqual(parsed, day_ow)
        self.assertEqual('till_next_day_of_week:Wednesday', str(day_ow))

        parsed = CacheExpire.parse_expire('till_next_day_of_week:Sunday')
        day_ow = TillNextDayOfWeek(6)
        self.assertEqual(parsed, day_ow)
        self.assertEqual('till_next_day_of_week:Sunday', str(day_ow))

    def test_has_expired_never(self):
        a = datetime(2000, 1, 1)
        b = datetime(3000, 1, 1)
        self.assertFalse(Never().has_expired(a, b))

    def test_has_expired_delta(self):
        a = datetime(2000, 1, 1)
        b = datetime(2000, 1, 1) + timedelta(days=4)
        self.assertTrue(Delta('days', 3).has_expired(a, b))

        b = datetime(2000, 1, 1) + timedelta(days=2)
        self.assertFalse(Delta('days', 3).has_expired(a, b))

        b = datetime(2000, 1, 1) + timedelta(days=3)
        self.assertFalse(Delta('days', 3).has_expired(a, b))

        b = datetime(2000, 1, 1) + timedelta(days=3, hours=1)
        self.assertTrue(Delta('days', 3).has_expired(a, b))

    def test_has_expired_next_day_of_week(self):
        a = datetime(2024, 9, 16) # a monday
        b = a + timedelta(days=7, hours=1)
        self.assertTrue(TillNextDayOfWeek(0).has_expired(a, b))

        b = a + timedelta(days=7)
        self.assertFalse(TillNextDayOfWeek(0).has_expired(a, b))

        b = a + timedelta(days=2)
        self.assertTrue(TillNextDayOfWeek(1).has_expired(a, b))
