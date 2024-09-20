from datetime import datetime, timedelta
from unittest import TestCase, IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from lib.service.clock.mocks import MockClockService
from lib.service.uuid.mocks import MockUuidService
from lib.service.io import IoService
from lib.service.http.cache import InstructionHeaders, CACHE_VERSION
from lib.service.http.cache.file_cache import RequestCache, FileCacher, RequestCacheFactory
from lib.service.http.cache.expiry import *

_file_path = 'blah/blah/blah'
_date_str = '2012-10-15 10:10:10'
_date_obj = datetime(2012, 10, 15, 10, 10, 10)
_empty_state_str = '{\n "version": %s,\n "files": {}\n}' % CACHE_VERSION

class RequestCacheTestCase(TestCase):
    factory = RequestCacheFactory(cache_dir='asdf')

    def test_parse_never_expire(self):
        obj = RequestCache(Never(), _file_path, _date_obj, cache_dir='asdf')
        dct = { 'expire': 'never', 'location': _file_path, 'age': _date_str }
        self.assertEqual(dct, RequestCache.to_json(obj))
        self.assertEqual(self.factory.from_json(dct), obj)

    def test_parse_delta_5_days(self):
        obj = RequestCache(Delta('days', 5), _file_path, _date_obj, cache_dir='asdf')
        dct = { 'expire': 'delta:days:5', 'location': _file_path, 'age': _date_str }
        self.assertEqual(dct, RequestCache.to_json(obj))
        self.assertEqual(self.factory.from_json(dct), obj)

    def test_parse_delta_5_days(self):
        obj = RequestCache(TillNextDayOfWeek(2), _file_path, _date_obj, cache_dir='asdf')
        dct = { 'expire': 'till_next_day_of_week:Wednesday', 'location': _file_path, 'age': _date_str }
        self.assertEqual(dct, RequestCache.to_json(obj))
        self.assertEqual(self.factory.from_json(dct), obj)

cache_dir = 'cache_dir'

class FileCacherTestCase(IsolatedAsyncioTestCase):
    mock_io = None

    @classmethod
    def _mk_state(cls, *args):
        state = {}
        for url, fmt, o in args:
            state[url] = {
                **state.get(url, {}),
                fmt: { 'expire': o[0], 'location': o[1], 'age': o[2] },
            }
        return state

    def _get_instance(self, state, uuid=None, clock=None):
        state_path = 'state_path'
        uuid = uuid or MockUuidService(values=['1', '2', '3'])
        clock = clock or MockClockService(dt=_date_obj)
        return FileCacher(cache_dir,
                          state_path,
                          RequestCacheFactory(cache_dir),
                          self.mock_io,
                          uuid,
                          clock,
                          state)

    def assertEqualIgnoringWhitespace(self, l, r):
        self.assertEqual(' '.join(l.split()), ' '.join(r.split()))

    async def asyncSetUp(self):
        self.mock_io = AsyncMock(spec=IoService)

    async def test_async_context(self):
        instance = self._get_instance(state=None)
        self.mock_io.f_read.return_value = _empty_state_str
        self.mock_io.f_write.return_value = None

        async with self._get_instance(state=None) as f:
            self.mock_io.f_read.assert_called_once_with('state_path')
            self.assertEqual(f._state, {})
        self.mock_io.f_write.assert_called_once_with('state_path', _empty_state_str)

    async def test_read_empty(self):
        instance = self._get_instance(state={})
        self.assertEqual(instance.read('a', 'json'), (None, False))

        instance = self._get_instance(state=self._mk_state())
        self.assertEqual(instance.read('a', 'json'), (None, False))

    async def test_read_valid(self):
        # 2012-12-12 = Wednesday, 2012-12-15 = Saturday
        a_date_str = '2012-12-12 10:10:10'
        a_date_obj = datetime(2012, 12, 12, 10, 10, 10)
        b_date_obj = datetime(2012, 12, 15, 10, 10, 10)

        clock = MockClockService(dt=b_date_obj)
        state = self._mk_state(
            ('a', 'json', ('never', 'file_a', a_date_str)),
            ('b', 'json', ('delta:days:5', 'file_b', a_date_str)),
            ('c', 'json', ('till_next_day_of_week:Sunday', 'file_c', a_date_str)),
        )

        instance = self._get_instance(state=state, clock=clock)

        self.assertEqual(
            instance.read('a', 'json'),
            ({ 'json': RequestCache(Never(), 'file_a', a_date_obj, cache_dir) }, True),
        )
        self.assertEqual(
            instance.read('b', 'json'),
            ({ 'json': RequestCache(Delta('days', 5), 'file_b', a_date_obj, cache_dir) }, True),
        )
        self.assertEqual(
            instance.read('c', 'json'),
            ({ 'json': RequestCache(TillNextDayOfWeek(6), 'file_c', a_date_obj, cache_dir) }, True),
        )

    async def test_read_expired(self):
        # 2012-12-12 = Wednesday, 2012-12-17 = Monday
        a_date_str = '2012-12-12 10:10:10'
        a_date_obj = datetime(2012, 12, 12, 10, 10, 10)
        b_date_obj = datetime(2012, 12, 17, 10, 10, 10)

        clock = MockClockService(dt=b_date_obj)
        state = self._mk_state(
            ('b', 'json', ('delta:days:4', 'file_b', a_date_str)),
            ('c', 'json', ('till_next_day_of_week:Sunday', 'file_c', a_date_str)),
        )

        instance = self._get_instance(state=state, clock=clock)

        self.assertEqual(
            instance.read('b', 'json'),
            ({ 'json': RequestCache(Delta('days', 4), 'file_b', a_date_obj, cache_dir) }, False),
        )
        self.assertEqual(
            instance.read('c', 'json'),
            ({ 'json': RequestCache(TillNextDayOfWeek(6), 'file_c', a_date_obj, cache_dir) }, False),
        )

    async def test_write_json_never_expire(self):
        request_meta = InstructionHeaders(format='json',
                                          expiry=Never(),
                                          disabled=False,
                                          request_label='fruitloop')
        request_data = '{"count":2012}'
        request_url = 'breakfast'
        uuid = MockUuidService(values=['u1'])
        clock = MockClockService(dt=_date_obj)
        fname = 'fruitloop-u1.json'
        decoded = RequestCache(Never(), fname, _date_obj, cache_dir=cache_dir)
        encoded = { 'expire': 'never', 'location': fname, 'age': _date_str }

        self.mock_io.f_write.return_value = None
        self.mock_io.f_delete.return_value = None
        instance = self._get_instance(state={}, uuid=uuid, clock=clock)
        cache = await instance.write(request_url, request_meta, request_data)
        self.assertEqual(cache, { 'json': decoded })
        self.assertEqual(instance._state, { 'breakfast': { 'json': encoded } })
        self.mock_io.f_write.assert_called_once_with(f'cache_dir/{fname}', request_data)
        self.mock_io.f_delete.assert_not_called()

    async def test_write_over_cached_resource(self):
        request_meta = InstructionHeaders(format='json',
                                          expiry=Never(),
                                          disabled=False,
                                          request_label='fruitloop')
        request_data = '{"count":2012}'
        request_url = 'breakfast'
        uuid = MockUuidService(values=['u1'])
        clock = MockClockService(dt=_date_obj)
        fname = 'fruitloop-u1.json'
        decoded = RequestCache(Never(), fname, _date_obj, 'cache_dir')
        encoded = { 'expire': 'never', 'location': fname, 'age': _date_str }

        self.mock_io.f_write.return_value = None
        self.mock_io.f_delete.return_value = None
        instance = self._get_instance(state={
            'breakfast': {
                'json': { **encoded, 'location': 'old-file' },
            }
        }, uuid=uuid, clock=clock)
        cache = await instance.write(request_url, request_meta, request_data)
        self.assertEqual(cache, { 'json': decoded })
        self.assertEqual(instance._state, { 'breakfast': { 'json': encoded } })
        self.mock_io.f_write.assert_called_once_with(f'cache_dir/{fname}', request_data)
        self.mock_io.f_delete.assert_called_once_with('cache_dir/old-file')




