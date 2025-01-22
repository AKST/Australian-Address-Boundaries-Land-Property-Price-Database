import pytest
from pprint import pformat

from lib.pipeline.nsw_lrs.property_description import data
from .. import types as t
from ..types import Folio
from ..parse import parse_land_parcel_ids
from ..parse import parse_property_description
from ..parse import parse_property_description_data

@pytest.mark.parametrize("name,desc", [
    ('001-mixed-parcels', 'B/100895 6, PT 20/755520'),
])
def test_parse_property_description_data(snapshot, name, desc):
    data = parse_property_description_data(desc)
    snapshot.assert_match(pformat(data), name)

@pytest.mark.parametrize("desc,remains,expected_items", [
    ('123//313', '', [Folio(id='123//313')]),
    ('123/313', '', [Folio(id='123/313')]),
    ('PT 123/313', '', [Folio(id='123/313', part=True)]),
    ('123/as/313', '', [Folio(id='123/as/313')]),
    ('1, 2/313', '', [
        Folio(id='1/313'),
        Folio(id='2/313'),
    ]),
    ('1, PT 2, 3/313', '', [
        Folio(id='1/313'),
        Folio(id='2/313', part=True),
        Folio(id='3/313'),
    ]),
    ('1, PT 2/123 PT 5, 3/313', '', [
        Folio(id='1/123'),
        Folio(id='2/123', part=True),
        Folio(id='5/313', part=True),
        Folio(id='3/313'),
    ]),
    ('PT 1/123 PT 2/223 PT 5/323', '', [
        Folio(id='1/123', part=True),
        Folio(id='2/223', part=True),
        Folio(id='5/323', part=True),
    ]),
])
def test_land_parcel_ids(desc, remains, expected_items):
    out_remains, out_items = parse_land_parcel_ids(desc)
    assert out_remains == remains
    assert out_items == expected_items

@pytest.mark.parametrize("desc,remains,expected_items", [
    ('6/G/12312 Permissive Occupancy 67/15', '', [
        Folio(id='6/G/12312'),
        t.PermissiveOccupancy('67/15'),
    ]),
    ('650/751743 Non-Irrigable Purchase 15', '', [
        Folio(id='650/751743'),
        t.NonIrrigablePurchase('15'),
    ]),
    ('PART; crown roads Licence 623573', 'PART; ', [
        t.CrownRoadsLicense(id='623573'),
    ]),
    (
        'PT 1/209581 PT 7321/1166558 Subsurface Area = 53.41ha; Surface Area = 12.25 ha Mining Lease 739',
        '; Surface Area = 12.25 ha ',
        [
            Folio(id='1/209581', part=True),
            Folio(id='7321/1166558', part=True),
            t.MiningLease(id='739'),
        ],
    ),
    ('B/100895 6, PT 20/755520 Enclosure Permit 510145', '', [
        Folio(id='B/100895'),
        Folio(id='6/755520'),
        Folio(id='20/755520', part=True),
        t.EnclosurePermit('510145'),
    ]),
    ('26/1066289 Western Land Lease 14476 Western Land Lease 31572', '', [
        Folio(id='26/1066289'),
        t.WesternLandLease('14476'),
        t.WesternLandLease('31572'),
    ]),
    ('PT 6401/1257392 Railway Land Lease 221.0037', '', [
        Folio(id='6401/1257392', part=True),
        t.RailwayLandLease('221.0037'),
    ]),
    ('PT 135, PT 210, PT 211/756913 Wind Farm AN614034', '', [
        Folio(id='135/756913', part=True),
        Folio(id='210/756913', part=True),
        Folio(id='211/756913', part=True),
        t.WindFarm('AN614034'),
    ]),
    ('98/1066289 Mineral Claim 30854 Western Land Lease 14457', '', [
        Folio(id='98/1066289'),
        t.MineralClaim('30854'),
        t.WesternLandLease('14457'),
    ]),
    ('25/7511 95.19/CRK', '95.19/CRK', [
        Folio(id='25/7511'),
    ]),
    ('1329/748788 PTARC/ARC20', 'PTARC/ARC20', [
        Folio(id='1329/748788'),
    ]),
    ('PT 200/713995 HCP9014/2', 'HCP9014/2', [
        Folio(id='200/713995', part=True),
    ]),
    ('1/804780 PM2005/000756', 'PM2005/000756', [
        Folio(id='1/804780'),
    ]),
    ('179/755241 Perpetual Lease 1935/1', '', [
        Folio(id='179/755241'),
        t.PerpetualLease('1935/1'),
    ]),
    (
        # TODO figure out what this is
        'PT 10/1142773 and lease - Waterways Authority 3313',
        'and lease - Waterways Authority 3313',
        [Folio(id='10/1142773', part=True)],
    ),
    ('10/10846 NSW Maritime 1000/1197', '', [
        Folio(id='10/10846'),
        t.NswMaritime('1000/1197'),
    ]),
    (
        '257, 258/722505 259, 260, 261/722511 262, 263/722516 1, 2, 3/728619 1, 2/728620 23, 25, 26, 36, 80, 81, 82, 83, 84, 90, 92, 94, 95, 97, 98, 251/756472 Licence over 258/722505, 260, 261/722511, 263/722516 Licence 396466',
        'Licence over 258/722505, 260, 261/722511, 263/722516 ',
        [
            Folio(id='257/722505'),
            Folio(id='258/722505'),
            Folio(id='259/722511'),
            Folio(id='260/722511'),
            Folio(id='261/722511'),
            Folio(id='262/722516'),
            Folio(id='263/722516'),
            Folio(id='1/728619'),
            Folio(id='2/728619'),
            Folio(id='3/728619'),
            Folio(id='1/728620'),
            Folio(id='2/728620'),
            *[
                Folio(id=f'{lot}/756472')
                for lot in [
                    23, 25, 26, 36, 80, 81, 82, 83,
                    84, 90, 92, 94, 95, 97, 98, 251,
                ]
            ],
            t.CrownLandLicense('396466'),
        ],
    ),
    ('41/753705 Lease Number 10 - 30', '', [Folio(id='41/753705'), t.LeaseNumber('10', '30')]),
    ('41/753705 Lease Number 10 TO 30', '', [Folio(id='41/753705'), t.LeaseNumber('10', '30')]),
    ('41/753705 Lease Number 10/30', '', [Folio(id='41/753705'), t.LeaseNumber('10/30', None)]),
    (
        '1/153580 1/198265 1/882247 1/883337 1/1007215 12, 13, 14/1139461 1/882247, 1/883337 and 1/1007215 COAL ONLY',
        '1/882247, 1/883337 and 1/1007215 COAL ONLY',
        [
            Folio(id='1/153580'),
            Folio(id='1/198265'),
            Folio(id='1/882247'),
            Folio(id='1/883337'),
            Folio(id='1/1007215'),
            Folio(id='12/1139461'),
            Folio(id='13/1139461'),
            Folio(id='14/1139461'),
        ]
    ),
    (
        '1/252283 6, 31/755497 25/755511 93, 102, /755532',
        '',
        [
            Folio(id='1/252283'),
            Folio(id='6/755497'),
            Folio(id='31/755497'),
            Folio(id='25/755511'),
            Folio(id='93/755532'),
            Folio(id='102/755532'),
        ]
    ),
    (
        '/13586 PH WAMMERA PT DP 13586 MEJUM STATE FOREST NO 378',
        '/13586 PH WAMMERA PT DP 13586 MEJUM STATE FOREST NO 378',
        [],
    ),
    (
        'Y/387072 Boatshed, Landing/Platform (Area 220.1m2)  Permissive Occupancy 59/80',
        'Boatshed, Landing/Platform (Area 220.1m2) ',
        [
            Folio(id='Y/387072'),
            t.PermissiveOccupancy('59/80'),
        ],
    ),
    (
        'PT 100/1101535 Previously2/514545 14/17 & pt 18/830834 & pt 245/793459',
        'Previously2/514545 14/17 & pt 18/830834 & pt 245/793459',
        [
            Folio(id='100/1101535', part=True),
        ],
    ),
    (
        '1, 2, 3, CP/SP 23170 1/80500 1/325917 LOT 1 DP 80500 & LOT 1 DP 325917 (BEING LOTS 1/3 SP 23170)',
        'LOT 1 DP 80500 & LOT 1 DP 325917 (BEING LOTS 1/3 SP 23170)',
        [
            Folio(id='1/SP23170'),
            Folio(id='2/SP23170'),
            Folio(id='3/SP23170'),
            Folio(id='CP/SP23170'),
            Folio(id='1/80500'),
            Folio(id='1/325917'),
        ],
    ),
    (
        '1/1062221 (being at Tarana in two parts:- Pumphouse (2042.5 m2); Reservoir 1/1062221 and Easement) Railway Land Lease 151311',
        '(being at Tarana in two parts:- Pumphouse (2042.5 m2); Reservoir 1/1062221 and Easement) ',
        [
            Folio(id='1/1062221'),
            t.RailwayLandLease('151311'),
        ],
    ),
    ('PT 2/1109126 Railway Land Lease 65/430/2470', '', [
        Folio(id='2/1109126', part=True),
        t.RailwayLandLease('65/430/2470'),
    ]),
    (
        '12/1161984 (part Wildlife Refuge No.362); Enclosure Permit 22427',
        '(part Wildlife Refuge No.362); ',
        [
            Folio(id='12/1161984'),
            t.EnclosurePermit(id='22427'),
        ],
    ),
    (
        '7309/1169890 5997/1205342 6058/1205343 6871/1205344 3578/1205346 PART; and crown roads Licence 623573',
        'PART; and ',
        [
            Folio(id='7309/1169890'),
            Folio(id='5997/1205342'),
            Folio(id='6058/1205343'),
            Folio(id='6871/1205344'),
            Folio(id='3578/1205346'),
            t.CrownRoadsLicense(id='623573'),
        ],
    ),
    ('1/1010832 Railway Land Lease 216.4101/430/3332 Railway Land Lease 65/430/3332', '', [
        Folio(id='1/1010832'),
        t.RailwayLandLease('216.4101/430/3332'),
        t.RailwayLandLease('65/430/3332'),
    ]),
    (
        '47, 50, 71, 116/756843 1/804117 102/1210073 1/1291983 (47,50,71,116/756843,102/1210073 being part of Cambalong Wildlife Refuge) Enclosure Permit 561155',
        '(47,50,71,116/756843,102/1210073 being part of Cambalong Wildlife Refuge) ',
        [
            Folio(id='47/756843'),
            Folio(id='50/756843'),
            Folio(id='71/756843'),
            Folio(id='116/756843'),
            Folio(id='1/804117'),
            Folio(id='102/1210073'),
            Folio(id='1/1291983'),
            t.EnclosurePermit('561155'),
        ],
    )
])
def test_parse_property_description(desc, remains, expected_items):
    out_remains, out_items = parse_property_description(desc)
    assert out_remains == remains
    assert out_items == expected_items

def test_unsupported_message_3838002():
    """
    Test for checking checking changes in `3838002`
    """
    property_desc = \
        '42, PT 57, PT 58, PT 59, PT 60, PT 61/754940 and Pt Leard ' \
        'State Forest No.420 in Parish of Leard; and then about ' \
        '1700 ha subsurface being Lots 3, pt 35, 55, Pts 58, 64, ' \
        '65 110 DP 754924; Lots 32, pt 35, 39, 40, 41, 42 DP 754940; ' \
        'Lots 27, 70, 74, 75, Pts 68, 69, 71, 83 DP 754948; Lots 1-2 ' \
        'DP 510801; pt 1 DP 1157540; pt 1, lot 3 DP 1144479; lot 1 ' \
        'DP114793; pt 7001 DP94069. Coal Lease 375 Mining Lease 1701'
    out_remains, out_items = parse_property_description(property_desc)
    assert out_items == [
        Folio(id='42/754940'),
        Folio(id='57/754940', part=True),
        Folio(id='58/754940', part=True),
        Folio(id='59/754940', part=True),
        Folio(id='60/754940', part=True),
        Folio(id='61/754940', part=True),
        t.MiningLease(id='1701'),
        t.CoalLease(id='375'),
    ]

def test_unsupported_message_3407923():
    """
    Test for checking checking changes in `3407923`
    """
    property_desc = \
        '122/42056 2/718634 2/733835 58, 69, 132/751078 1, 2/1022767 ' \
        '1, 4, 5/1080470 for grazing(crown land bounded by 105/751078, ' \
        '1,2/1080470;parcel west of 96/751078) Licence 344585 Licence 405371'
    out_remains, out_items = parse_property_description(property_desc)
    assert out_items == [
        Folio(id='122/42056'),
        Folio(id='2/718634'),
        Folio(id='2/733835'),
        Folio(id='58/751078'),
        Folio(id='69/751078'),
        Folio(id='132/751078'),
        Folio(id='1/1022767'),
        Folio(id='2/1022767'),
        Folio(id='1/1080470'),
        Folio(id='4/1080470'),
        Folio(id='5/1080470'),
        t.CrownLandLicense(id='344585'),
        t.CrownLandLicense(id='405371'),
    ]

def test_unsupported_message_748784():
    """
    Test for checking checking changes in `748784`
    """
    property_desc = \
        '252/531397 Council lease (34sqm. - STONE SEAWA;L, '\
        'RECLAIMED LAND, BOATSHED, DECK T/SKID) SHARED USE '\
        'NSW Maritime 30003836'
    out_remains, out_items = parse_property_description(property_desc)
    assert out_items == [
        Folio(id='252/531397'),
        t.NswMaritime(id='30003836'),
    ]

def test_unsupported_message_970426():
    """
    Test for checking checking changes in `970426`
    """
    property_desc = \
        'PT 7093/93909 AND NSW MARITIME LEASE SHOWN IN ' \
        'PLAN No WL2025; BEING THE SITE OF NORTHBRIDGE ' \
        'SAILING CLUB, RAMP, PONTOON, CONCRETE PILES AND ' \
        'CHAIN STAYS. PART LOT 7093 DP 93909 HAS AN UNSPECIFIED AREA.'
    out_remains, out_items = parse_property_description(property_desc)
    assert out_items == [
        Folio(id='7093/93909', part=True),
    ]

