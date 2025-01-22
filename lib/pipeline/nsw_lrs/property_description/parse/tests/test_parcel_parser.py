import pytest

from lib.pipeline.nsw_lrs.property_description import data
from ..parcel_parser import parse_parcel_data

@pytest.mark.parametrize("p_in,p_out", [
    ('B/12313', data.Folio('B/12313', 'B', None, '12313'))
])
def test_parse_parcel(snapshot, p_in, p_out):
    assert parse_parcel_data(p_in) == p_out
