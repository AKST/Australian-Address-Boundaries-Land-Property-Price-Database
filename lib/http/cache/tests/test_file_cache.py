from datetime import datetime, timedelta
import unittest

from lib.http.cache.file_cache import RequestCache
from lib.http.cache.expiry import *

_file_path = 'blah/blah/blah'
_date_str = '2012-10-15 10:10:10'
_date_obj = datetime(2012, 10, 15, 10, 10, 10)

class RequestCacheTestCase(unittest.TestCase):
    def test_parse_never_expire(self):
        obj = RequestCache(Never(), _file_path, _date_obj)
        dct = { 'expire': 'never', 'location': _file_path, 'age': _date_str }
        self.assertEqual(dct, RequestCache.to_json(obj))
        self.assertEqual(RequestCache.from_json(dct), obj)

    def test_parse_delta_5_days(self):
        obj = RequestCache(Delta('days', 5), _file_path, _date_obj)
        dct = { 'expire': 'delta:days:5', 'location': _file_path, 'age': _date_str }
        self.assertEqual(dct, RequestCache.to_json(obj))
        self.assertEqual(RequestCache.from_json(dct), obj)

    def test_parse_delta_5_days(self):
        obj = RequestCache(TillNextDayOfWeek(2), _file_path, _date_obj)
        dct = { 'expire': 'till_next_day_of_week:Wednesday', 'location': _file_path, 'age': _date_str }
        self.assertEqual(dct, RequestCache.to_json(obj))
        self.assertEqual(RequestCache.from_json(dct), obj)



