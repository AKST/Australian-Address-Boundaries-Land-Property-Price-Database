from datetime import datetime
import unittest
from unittest.mock import Mock

from lib.service.clock.mocks import MockClockService
import lib.gis.predicate as p
from lib.gis.predicate import DateRangeParam
from lib.gis.predicate import YearMonth as YM
from lib.gis.predicate.date import DateRangeParam

mock_dt = datetime(2012, 10, 1)
get_now = lambda: datetime(2012, 10, 1)

class DateRangeParamTestCase(unittest.TestCase):
    mock_clock = None

    def setUp(self):
        self.mock_clock = MockClockService(dt=mock_dt)

    def test_can_cache(self):
        param = DateRangeParam(YM(2010, 1), YM(2011, 1), self.mock_clock)
        self.assertTrue(param.can_cache())

        param = DateRangeParam(YM(2010, 1), YM(2013, 1), self.mock_clock)
        self.assertFalse(param.can_cache())

    def test_shard(self):
        m12_apart = DateRangeParam(YM(2010, 5), YM(2011, 5), self.mock_clock)
        self.assertEqual(list(m12_apart.shard()), [
            m12_apart._scoped(YM(2010, 5), YM(2010, 6)),
            m12_apart._scoped(YM(2010, 6), YM(2010, 7)),
            m12_apart._scoped(YM(2010, 7), YM(2010, 8)),
            m12_apart._scoped(YM(2010, 8), YM(2010, 9)),
            m12_apart._scoped(YM(2010, 9), YM(2010, 10)),
            m12_apart._scoped(YM(2010, 10), YM(2010, 11)),
            m12_apart._scoped(YM(2010, 11), YM(2010, 12)),
            m12_apart._scoped(YM(2010, 12), YM(2011, 1)),
            m12_apart._scoped(YM(2011, 1), YM(2011, 2)),
            m12_apart._scoped(YM(2011, 2), YM(2011, 3)),
            m12_apart._scoped(YM(2011, 3), YM(2011, 4)),
            m12_apart._scoped(YM(2011, 4), YM(2011, 5)),
        ])

        y20_apart = DateRangeParam(YM(1950, 1), YM(1970, 1), self.mock_clock)
        self.assertEqual(list(y20_apart.shard()), [
            y20_apart._scoped(YM(1950, 1), YM(1955, 1)),
            y20_apart._scoped(YM(1955, 1), YM(1960, 1)),
            y20_apart._scoped(YM(1960, 1), YM(1965, 1)),
            y20_apart._scoped(YM(1965, 1), YM(1970, 1)),
        ])

        y21_apart = DateRangeParam(YM(1950, 1), YM(1971, 1), self.mock_clock)
        self.assertEqual(list(y21_apart.shard()), [
            y21_apart._scoped(YM(1950, 1), YM(1955, 1)),
            y21_apart._scoped(YM(1955, 1), YM(1960, 1)),
            y21_apart._scoped(YM(1960, 1), YM(1965, 1)),
            y21_apart._scoped(YM(1965, 1), YM(1970, 1)),
            y21_apart._scoped(YM(1970, 1), YM(1971, 1)),
        ])

        y300_apart = DateRangeParam(YM(1000, 1), YM(1300, 1), self.mock_clock)
        self.assertEqual(list(y300_apart.shard()), [
            y300_apart._scoped(YM(1000, 1), YM(1100, 1)),
            y300_apart._scoped(YM(1100, 1), YM(1200, 1)),
            y300_apart._scoped(YM(1200, 1), YM(1300, 1)),
        ])

        y301_apart = DateRangeParam(YM(1000, 1), YM(1301, 1), self.mock_clock)
        self.assertEqual(list(y301_apart.shard()), [
            y301_apart._scoped(YM(1000, 1), YM(1100, 1)),
            y301_apart._scoped(YM(1100, 1), YM(1200, 1)),
            y301_apart._scoped(YM(1200, 1), YM(1300, 1)),
            y301_apart._scoped(YM(1300, 1), YM(1301, 1)),
        ])
