from dataclasses import dataclass, field
from typing import List, Union, Any, Optional
import re

from lib.nsw_vg.property_description import types as t

@dataclass
class IdPattern:
    re: Any
    Const: Any
    
@dataclass
class NamePattern:
    re: Any
    id_names: List[str]
    Const: Any
    bool_names: List[bool] = field(default_factory=lambda: [])
    
@dataclass
class FlagPattern:
    re: Any
    Const: Any

id_patterns = [
    IdPattern(re=re.compile(r'Wind Farm\s+(\w+)'), Const=t.WindFarm),
    IdPattern(re=re.compile(r"Consolidated Mining Lease\s+(\w+)"), Const=t.ConsolidatedMiningLease),

    
    IdPattern(re=re.compile(r'Public Reserve\s+(\w+)'), Const=t.PublicReserve),
    IdPattern(re=re.compile(r'Perpetual Lease\s+(\w+)'), Const=t.PerpetualLease),
    
    
    # there doesn't appear to be such a thing as a mining permit
    #
    # https://ablis.business.gov.au/search/customsearch
    IdPattern(re=re.compile(r'Mining Permit\s+(\w+)'), Const=t.MiningLease),
    IdPattern(re=re.compile(r'Telstra Site Number\s+(\w+)'), Const=t.TelstraSite),
    IdPattern(re=re.compile(r'Mineral Claim\s+(\w+)'), Const=t.MineralClaim),
    IdPattern(re=re.compile(r'Western Land Lease\s+(\w+)'), Const=t.WesternLandLease),
    IdPattern(re=re.compile(r'Railway Land Lease\s+(\w+(\.\w+)?)'), Const=t.RailwayLandLease),
    IdPattern(re=re.compile(r'Stock Watering Place\s+(\w+)'), Const=t.StockWaterPlaceLease),
    IdPattern(re=re.compile(r'Special Lease\s+(\w+)'), Const=t.SpecialLease),
    IdPattern(re=re.compile(r'Forest Permit\s+(\w+)'), Const=t.ForestPermit),
    IdPattern(re=re.compile(r'Enclosure Permit\s+(\w+)'), Const=t.EnclosurePermit),
    IdPattern(re=re.compile(r'Non-Irrigable Purchase\s+(\w+)'), Const=t.NonIrrigablePurchase),
    IdPattern(re=re.compile(r'NSW Maritime\s+(\w+)'), Const=t.NswMaritime),
    IdPattern(re=re.compile(r'Housing PRN\s+(\w+)'), Const=t.HousingPRN),
    IdPattern(re=re.compile(r'Licence\s+(\w+)'), Const=t.CrownLandLicense),
    IdPattern(re=re.compile(r'BUS DEPOT LEASE\s+(\w+)'), Const=t.BusDepotLease),
    
    # IdPattern(re=re.compile(r'STATE HERITAGE REGISTRAR\s+(\w+)', re.IGNORECASE), Const=StateHeritageRegister),
    IdPattern(re=re.compile(r'State Heritage Listing No\s+(\w+)', re.IGNORECASE), Const=t.StateHeritageRegister),
    IdPattern(re=re.compile(r'Permissive Occupancy\s+(\w+)'), Const=t.PermissiveOccupancy),
    IdPattern(re=re.compile(r'RAILCORP\. FILE:\s+(\w+)', re.IGNORECASE), Const=t.RailcorpFile),
    IdPattern(re=re.compile(r'Domestic Waterfront Occupancy\s+(\w+)'), Const=t.DomesticWaterfrontOccupation),
    
    # I am guessing these are the same things
    IdPattern(re=re.compile(r'Occupation Permit PB\s+(\w+)'), Const=t.OccupancyPermit),
    IdPattern(re=re.compile(r'Occupation Permit\s+(\w+)'), Const=t.OccupancyPermit),
    IdPattern(re=re.compile(r'Occupancy Permit\s+(\w+)'), Const=t.OccupancyPermit),
]

named_group_patterns = [
    NamePattern(
        re=re.compile(r'State Heritage\s+(Register|REGISTRAR)\s+(SHR\s+NO\.\s+)?(?P<id>\w+)', re.IGNORECASE),
        id_names=['id'],
        Const=t.StateHeritageRegister,
    ),
    NamePattern(
        re=re.compile(r'Subject to (SHR|SHRL) No\s+(?P<id>\w+)', re.IGNORECASE),
        id_names=['id'],
        Const=t.StateHeritageRegister,
    ),
    NamePattern(
        re=re.compile(r'Crown Reserve\s+(?P<id>\w+)'),
        id_names=['id'],
        Const=t.CrownReserve,
    ),
    NamePattern(
        re=re.compile(r'Part Crown Plan\s+(?P<id_a>\w+)-(?P<id_b>\w+)'),
        id_names=['id_a', 'id_b'],
        Const=lambda **kwargs: t.CrownPlan(**kwargs, part=True),
    ),
    NamePattern(
        re=re.compile(r'Crown Plan\s+(?P<id_a>\w+)-(?P<id_b>\w+)(?P<part>\s+\(Part\))?'),
        id_names=['id_a', 'id_b'],
        bool_names=['part'],
        Const=t.CrownPlan,
    ),
    # Mining Lease — You will need a mining lease to extract minerals for the PURPOSE of commercial mining
    # via: https://ablis.business.gov.au/service/nsw/mining-lease/16580
    NamePattern(
        re=re.compile(r'Mining (Purpose )?Lease\s+(?P<id>\w+)(?P<part>\s+\(Part\))?', re.IGNORECASE), 
        id_names=['id'],
        bool_names=['part'],
        Const=t.MiningLease,
    ),
    NamePattern(
        re=re.compile(r"Consolidated Coal Lease\s+(?P<id>\w+)(?P<part>\s+\(Part\))?", re.IGNORECASE), 
        id_names=['id'],
        bool_names=['part'],
        Const=t.ConsolidatedCoalLease,
    ),
    NamePattern(
        re=re.compile(r'Coal Lease\s+(?P<id>\w+)(?P<part>\s+\(Part\))?', re.IGNORECASE), 
        id_names=['id'],
        bool_names=['part'],
        Const=t.CoalLease,
    ),
    NamePattern(
        re=re.compile(r'Mineral Lease\s+(?P<id>\w+)(?P<part>\s+\(Part\))?', re.IGNORECASE), 
        id_names=['id'],
        bool_names=['part'],
        Const=t.MineralLease,
    ),
    NamePattern(
        re=re.compile(r'Lease Number\s+(?P<id_a>\w+) TO (?P<id_b>\w+)'),
        id_names=['id_a', 'id_b'],
        Const=t.LeaseNumber,
    ),
    NamePattern(
        re=re.compile(r'Lease Number\s+(?P<id_a>\w+)( - (?P<id_b>\w+))?'),
        id_names=['id_a', 'id_b'],
        Const=t.LeaseNumber,
    ),
    NamePattern(
        re=re.compile(r'Site\s+(?P<site>\w+) of Sydney Ports Corporation Plan\s+(?P<plan>\w+)'),
        id_names=['site', 'plan'],
        Const=t.SydneyPortsCorporationPlan,
    ),
]

flag_patterns = [
    FlagPattern(re=re.compile('DRAINAGE RESERVE'), Const=t.IsDrainageReserve),
]

ignore_pre_patterns = [
    re.compile(r"Licence for grazing"),
    re.compile(r"NSW Maritime Lease of \w+ sqm('s)?", re.IGNORECASE)
]

ignore_post_patterns = [
    # TOTAL SUBSURFACE AREA = 1090.5 HA
    re.compile(r"(TOTAL )?SU(B)?SURFACE\s+(AREA\s+)?(=|-)?\s+(\w+)(\.\w+)?(\s+)?HA", re.IGNORECASE),
    re.compile(r"UNDEFINED ROAD RESERVE"),
    re.compile(r"& road reserve", re.IGNORECASE),
    re.compile(r"Crown Road"),
    re.compile(r"THE WANGANELLA WILDLIFE REFUGE NO \w+", re.IGNORECASE),
    re.compile(r"COONONG WILDLIFE REFUGE (NO )?\w+", re.IGNORECASE),
    re.compile(r'(Partly )?(Un)?Lim(it|ti)ed in height (and|but|\&) ((Un)?lim(it|ti)ed in )?depth', re.IGNORECASE),
    re.compile(r'(Partly )?(Un)?Lim(it|ti)ed in depth (and|but|\&) ((Un)?lim(it|ti)ed in )?height', re.IGNORECASE),
    re.compile(r'(Partly )?(Un)?Lim(it|ti)ed in (depth|height)', re.IGNORECASE),
    re.compile(r'LEASE ATTACHED TO PROPERTY', re.IGNORECASE),
    re.compile(r'Limited in Stratum', re.IGNORECASE),
    re.compile(r'(lease|least) (OVER|OVE) property', re.IGNORECASE),
    re.compile(r'PROPERTY (OVER|OVE) LEASE', re.IGNORECASE),
    re.compile(r'SUBSURFACE ONLY', re.IGNORECASE),
    re.compile(r'LOT \w+ DP \w+ MINERAL ONLY', re.IGNORECASE),
    re.compile(r'COAL ONLY PLAN - \w+', re.IGNORECASE),
    
    # only appears on 3 different properties
    re.compile(r'Share Use', re.IGNORECASE),
    re.compile(r'Shared Use', re.IGNORECASE), 
    re.compile(r'EXCLUDING SURFACE LAND VALUED ON OCCUPATION', re.IGNORECASE),

    # I saw this stand alone after a parcel number with zero context.
    re.compile(r'(Unleased )?floor space area'), 
    
    # idk what's going with this...
    # it's often trailing after licenses of permits
    # re.compile(r'(PART)'), 
]