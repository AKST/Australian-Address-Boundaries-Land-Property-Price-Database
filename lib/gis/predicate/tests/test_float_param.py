import unittest
from unittest.mock import Mock

from lib.gis.predicate import FloatRangeParam

class FloatParamTestCase(unittest.TestCase):
    def test_shard_100_to_200(self):
        param = FloatRangeParam(100, 200)
        self.assertEqual(list(param.shard()), [
            FloatRangeParam(100.0, 106.0),
            FloatRangeParam(106.0, 113.0),
            FloatRangeParam(113.0, 120.0),
            FloatRangeParam(120.0, 128.0),
            FloatRangeParam(128.0, 137.0),
            FloatRangeParam(137.0, 145.0),
            FloatRangeParam(145.0, 155.0),
            FloatRangeParam(155.0, 165.0),
            FloatRangeParam(165.0, 176.0),
            FloatRangeParam(176.0, 187.0),
            FloatRangeParam(187.0, 200.0),
        ])

    def test_shard_1_to_12(self):
        param = FloatRangeParam(1, 12)
        self.assertEqual(list(param.shard()), [
            FloatRangeParam(1, 1.2),
            FloatRangeParam(1.2, 1.5),
            FloatRangeParam(1.5, 1.9),
            FloatRangeParam(1.9, 2.0),
            FloatRangeParam(2.0, 3.0),
            FloatRangeParam(3.0, 3.8),
            FloatRangeParam(3.8, 4.0),
            FloatRangeParam(4.0, 6.0),
            FloatRangeParam(6.0, 7.0),
            FloatRangeParam(7.0, 9.0),
            FloatRangeParam(9.0, 12.0),
        ])

    def test_shard_1_to_2(self):
        param = FloatRangeParam(1, 2)
        self.assertEqual(list(param.shard()), [
            FloatRangeParam(1, 1.06),
            FloatRangeParam(1.06, 1.1),
            FloatRangeParam(1.1, 1.2),
            FloatRangeParam(1.2, 1.28),
            FloatRangeParam(1.28, 1.3),
            FloatRangeParam(1.3, 1.4),
            FloatRangeParam(1.4, 1.5),
            FloatRangeParam(1.5, 1.6),
            FloatRangeParam(1.6, 1.7),
            FloatRangeParam(1.7, 1.8),
            FloatRangeParam(1.8, 2.0),
        ])

    def test_shard_1p5_to_2p5(self):
        param = FloatRangeParam(1.55555, 2.55555)
        self.assertEqual(list(param.shard()), [
            FloatRangeParam(1.55555, 1.6),
            FloatRangeParam(1.6, 1.7),
            FloatRangeParam(1.7, 1.78),
            FloatRangeParam(1.78, 1.8),
            FloatRangeParam(1.8, 1.9),
            FloatRangeParam(1.9, 2.0),
            FloatRangeParam(2.0, 2.1),
            FloatRangeParam(2.1, 2.2),
            FloatRangeParam(2.2, 2.3),
            FloatRangeParam(2.3, 2.4),
            FloatRangeParam(2.4, 2.55555),
        ])

