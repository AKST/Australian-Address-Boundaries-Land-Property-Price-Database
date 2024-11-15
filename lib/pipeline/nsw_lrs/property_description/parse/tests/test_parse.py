import pytest
from pprint import pformat

from lib.pipeline.nsw_lrs.property_description import data
from .. import types as t
from ..types import LandParcel
from ..parse import parse_parcel
from ..parse import parse_land_parcel_ids
from ..parse import parse_property_description
from ..parse import parse_property_description_data

@pytest.mark.parametrize("name,desc", [
    ('001-mixed-parcels', 'B/100895 6, PT 20/755520'),
])
def test_parse_property_description_data(snapshot, name, desc):
    data = parse_property_description_data(desc)
    snapshot.assert_match(pformat(data), name)

@pytest.mark.parametrize("p_in,p_out", [
    ('B/12313', data.LandParcel('B/12313', 'B', None, '12313'))
])
def test_parse_parcel(snapshot, p_in, p_out):
    assert parse_parcel(p_in) == p_out

@pytest.mark.parametrize("desc,remains,expected_items", [
    ('123//313', '', [LandParcel(id='123//313')]),
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
    ('6/G/12312 Permissive Occupancy 67/15', '', [
        t.PermissiveOccupancy('67/15'),
        LandParcel(id='6/G/12312'),
    ]),
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
    ('25/7511 95.19/CRK', '95.19/CRK', [
        LandParcel(id='25/7511'),
    ]),
    ('PT 200/713995 HCP9014/2', 'HCP9014/2', [
        LandParcel(id='200/713995', part=True),
    ]),
    ('PT 2/1109126 Railway Land Lease 65/430/2470', '', [
        t.RailwayLandLease('65/430/2470'),
        LandParcel(id='2/1109126', part=True),
    ]),
    (
        '47, 50, 71, 116/756843 1/804117 102/1210073 1/1291983 (47,50,71,116/756843,102/1210073 being part of Cambalong Wildlife Refuge) Enclosure Permit 561155',
        '(47,50,71,116/756843,102/1210073 being part of Cambalong Wildlife Refuge) ',
        [
            t.EnclosurePermit('561155'),
            LandParcel(id='47/756843'),
            LandParcel(id='50/756843'),
            LandParcel(id='71/756843'),
            LandParcel(id='116/756843'),
            LandParcel(id='1/804117'),
            LandParcel(id='102/1210073'),
            LandParcel(id='1/1291983'),
        ],
    )
])
def test_parse_property_description(desc, remains, expected_items):
    out_remains, out_items = parse_property_description(desc)
    assert out_remains == remains
    assert out_items == expected_items

