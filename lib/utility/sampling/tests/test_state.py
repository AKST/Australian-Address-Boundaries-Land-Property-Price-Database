import pytest

from lib.service.clock import AbstractClockService
from lib.service.clock.mocks import MockClockService

from .. import *
from ..base import FloatSample


@pytest.fixture
def clock() -> AbstractClockService:
    from datetime import datetime
    dt = datetime(2012, 12, 12, 12, 12, 12)
    return MockClockService(dt=dt, clock_time=0)

@pytest.mark.parametrize("observed,state,previous", [
    (
        4,
        SampleState(FloatSample(10.0)),
        Sample(FloatSample(5.0), observed=2, previous=None, last=None),
    ),
    (
        6,
        SampleState(FloatSample(15.0)),
        Sample(
            FloatSample(10.0),
            observed=4,
            previous=Sample(
                FloatSample(5.0),
                observed=2,
                previous=None,
                last=None,
            ),
            last=Sample(
                FloatSample(5.0),
                previous=None,
                observed=2,
                last=None,
            ),
        ),
    ),
])
def test_sample_chain(observed, state, previous):
    assert Sample.chain(observed, state, previous) == Sample(
        state=state.state,
        observed=observed,
        previous=Sample.copy(previous),
        last=previous.last or previous,
    )

def test_truncate_at_inclusive_bound():
    c_sample = Sample(FloatSample(1), observed=0)
    b_sample = Sample(FloatSample(2), observed=15, previous=c_sample)
    a_sample = Sample(FloatSample(3), observed=30, previous=b_sample, last=c_sample)
    a_sample.truncate(SamplingConfig(max_duration=30, max_sample_depth=5))
    assert b_sample.previous == c_sample
    assert a_sample.last == c_sample

def test_truncate_out_of_bounds():
    c_sample = Sample(FloatSample(1), observed=0)
    b_sample = Sample(FloatSample(2), observed=15, previous=c_sample)
    a_sample = Sample(FloatSample(3), observed=30, previous=b_sample, last=c_sample)
    a_sample.truncate(SamplingConfig(max_duration=29, max_sample_depth=5))
    assert b_sample.previous == None
    assert a_sample.last == b_sample

def test_truncate_depth():
    c_sample = Sample(FloatSample(1), observed=0)
    b_sample = Sample(FloatSample(2), observed=15, previous=c_sample)
    a_sample = Sample(FloatSample(3), observed=30, previous=b_sample, last=c_sample)
    a_sample.truncate(SamplingConfig(max_duration=30, max_sample_depth=2))
    assert b_sample.previous == None
    assert a_sample.last == b_sample

def test_sample_rpm():
    b_sample = Sample(FloatSample(0.0), observed=0, previous=None)
    a_sample = Sample(FloatSample(600.0), observed=60, previous=b_sample, last=b_sample)
    assert a_sample.rpm() == FloatSample(10.0)

