from datetime import datetime, timedelta
import unittest

from lib.http.cached_http import CacheExpire, RequestCache

class CacheExpireTestCase(unittest.TestCase):
    def test_parse_never(self):
        parsed = CacheExpire.parse_expire('never')
        self.assertEqual(parsed, CacheExpire.Never())
        self.assertEqual('never', str(CacheExpire.Never()))

    def test_parse_delta(self):
        parsed = CacheExpire.parse_expire('delta:days:5')
        self.assertEqual(parsed, CacheExpire.Delta('days', 5))
        self.assertEqual('delta:days:5', str(CacheExpire.Delta('days', 5)))

        parsed = CacheExpire.parse_expire('delta:weeks:2')
        self.assertEqual(parsed, CacheExpire.Delta('weeks', 2))
        self.assertEqual('delta:weeks:2', str(CacheExpire.Delta('weeks', 2)))

    def test_parse_till_next_day_of_week(self):
        parsed = CacheExpire.parse_expire('till_next_day_of_week:Monday')
        day_ow = CacheExpire.TillNextDayOfWeek(0)
        self.assertEqual(parsed, day_ow)
        self.assertEqual('till_next_day_of_week:Monday', str(day_ow))

        parsed = CacheExpire.parse_expire('till_next_day_of_week:Wednesday')
        day_ow = CacheExpire.TillNextDayOfWeek(2)
        self.assertEqual(parsed, day_ow)
        self.assertEqual('till_next_day_of_week:Wednesday', str(day_ow))

        parsed = CacheExpire.parse_expire('till_next_day_of_week:Sunday')
        day_ow = CacheExpire.TillNextDayOfWeek(6)
        self.assertEqual(parsed, day_ow)
        self.assertEqual('till_next_day_of_week:Sunday', str(day_ow))

    def test_has_expired_never(self):
        a = datetime(2000, 1, 1)
        b = datetime(3000, 1, 1)
        self.assertFalse(CacheExpire.Never().has_expired(a, b))

    def test_has_expired_delta(self):
        a = datetime(2000, 1, 1)
        b = datetime(2000, 1, 1) + timedelta(days=4)
        self.assertTrue(CacheExpire.Delta('days', 3).has_expired(a, b))

        b = datetime(2000, 1, 1) + timedelta(days=2)
        self.assertFalse(CacheExpire.Delta('days', 3).has_expired(a, b))

        b = datetime(2000, 1, 1) + timedelta(days=3)
        self.assertFalse(CacheExpire.Delta('days', 3).has_expired(a, b))

        b = datetime(2000, 1, 1) + timedelta(days=3, hours=1)
        self.assertTrue(CacheExpire.Delta('days', 3).has_expired(a, b))

    def test_has_expired_next_day_of_week(self):
        a = datetime(2024, 9, 16) # a monday
        b = a + timedelta(days=7, hours=1)
        self.assertTrue(CacheExpire.TillNextDayOfWeek(0).has_expired(a, b))

        b = a + timedelta(days=7)
        self.assertFalse(CacheExpire.TillNextDayOfWeek(0).has_expired(a, b))

        b = a + timedelta(days=2)
        self.assertTrue(CacheExpire.TillNextDayOfWeek(1).has_expired(a, b))

_file_path = 'blah/blah/blah'
_date_str = '2012-10-15 10:10:10'
_date_obj = datetime(2012, 10, 15, 10, 10, 10)

class RequestCacheTestCase(unittest.TestCase):
    def test_parse_never_expire(self):
        obj = RequestCache(CacheExpire.Never(), _file_path, _date_obj)
        dct = { 'expire': 'never', 'location': _file_path, 'age': _date_str }
        self.assertEqual(dct, RequestCache.to_json(obj))
        self.assertEqual(RequestCache.from_json(dct), obj)

    def test_parse_delta_5_days(self):
        obj = RequestCache(CacheExpire.Delta('days', 5), _file_path, _date_obj)
        dct = { 'expire': 'delta:days:5', 'location': _file_path, 'age': _date_str }
        self.assertEqual(dct, RequestCache.to_json(obj))
        self.assertEqual(RequestCache.from_json(dct), obj)

    def test_parse_delta_5_days(self):
        obj = RequestCache(CacheExpire.TillNextDayOfWeek(2), _file_path, _date_obj)
        dct = { 'expire': 'till_next_day_of_week:Wednesday', 'location': _file_path, 'age': _date_str }
        self.assertEqual(dct, RequestCache.to_json(obj))
        self.assertEqual(RequestCache.from_json(dct), obj)



