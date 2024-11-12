import pytest

from lib.pipeline.nsw_vg.property_description import types as t
from lib.pipeline.nsw_vg.property_description.types import LandParcel

from ..parse import parse_land_parcel_ids
from ..parse import parse_property_description

@pytest.mark.parametrize("desc,remains,expected_items", [
    ('123/313', '', [LandParcel(id='123/313')]),
    ('PT 123/313', '', [LandParcel(id='123/313', part=True)]),
    ('123/as/313', '', [LandParcel(id='123/as/313')]),
    ('1, 2/313', '', [
        LandParcel(id='1/313'),
        LandParcel(id='2/313'),
    ]),
    ('1, PT 2, 3/313', '', [
        LandParcel(id='1/313'),
        LandParcel(id='2/313', part=True),
        LandParcel(id='3/313'),
    ]),
    ('1, PT 2/123 PT 5, 3/313', '', [
        LandParcel(id='1/123'),
        LandParcel(id='2/123', part=True),
        LandParcel(id='5/313', part=True),
        LandParcel(id='3/313'),
    ]),
    ('PT 1/123 PT 2/223 PT 5/323', '', [
        LandParcel(id='1/123', part=True),
        LandParcel(id='2/223', part=True),
        LandParcel(id='5/323', part=True),
    ]),
])
def test_land_parcel_ids(desc, remains, expected_items):
    out_remains, out_items = parse_land_parcel_ids(desc)
    assert out_remains == remains
    assert out_items == expected_items

@pytest.mark.parametrize("desc,remains,expected_items", [
    ('650/751743 Non-Irrigable Purchase 15', '', [
        t.NonIrrigablePurchase('15'),
        LandParcel(id='650/751743'),
    ]),
    ('B/100895 6, PT 20/755520 Enclosure Permit 510145', '', [
        t.EnclosurePermit('510145'),
        LandParcel(id='B/100895'),
        LandParcel(id='6/755520'),
        LandParcel(id='20/755520', part=True),
    ]),
    ('26/1066289 Western Land Lease 14476 Western Land Lease 31572', '', [
        t.WesternLandLease('14476'),
        t.WesternLandLease('31572'),
        LandParcel(id='26/1066289'),
    ]),
    ('PT 6401/1257392 Railway Land Lease 221.0037', '', [
        t.RailwayLandLease('221.0037'),
        LandParcel(id='6401/1257392', part=True),
    ]),
    ('PT 135, PT 210, PT 211/756913 Wind Farm AN614034', '', [
        t.WindFarm('AN614034'),
        LandParcel(id='135/756913', part=True),
        LandParcel(id='210/756913', part=True),
        LandParcel(id='211/756913', part=True),
    ]),
    ('98/1066289 Mineral Claim 30854 Western Land Lease 14457', '', [
        t.MineralClaim('30854'),
        t.WesternLandLease('14457'),
        LandParcel(id='98/1066289'),
    ]),
])
def test_parse_property_description(desc, remains, expected_items):
    out_remains, out_items = parse_property_description(desc)
    assert out_remains == remains
    assert out_items == expected_items
