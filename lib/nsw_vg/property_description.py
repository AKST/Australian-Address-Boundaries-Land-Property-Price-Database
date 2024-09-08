from dataclasses import dataclass, field
from collections import namedtuple
from typing import List, Union, Any, Optional
import re

@dataclass
class LandParcel:
    id: str
    part: bool = field(default=False)
    

PublicReserve = namedtuple("PublicReserve", ['id'])
BusDepotLease = namedtuple("BusDepotLease", ['id'])
WindFarm = namedtuple("WindFarm", ['id'])
TelstraSite = namedtuple("TelstraSite", ['id'])
SydneyPortsCorporationPlan = namedtuple('SydneyPortsCorporationPlan', ['site', 'plan'])

@dataclass
class CoalLease:
    """
    For anything matching the grammar "Coal Lease \w+"
    """
    id: str
    part: Optional[bool] = field(default=False)

@dataclass
class ConsolidatedCoalLease:
    """
    For anything matching the grammar "Consolidated Coal Lease \w+"
    """
    id: str
    part: bool

@dataclass
class ConsolidatedMiningLease:
    """
    For anythign matching the grammar "Consolidated Mining Lease \w+"
    """
    id: str

# https://ablis.business.gov.au/service/nsw/mining-lease/16580
@dataclass
class MiningLease:
    """
    For anything matching the grammar "Mining Lease \w+"
    """
    id: str
    part: Optional[bool] = field(default=False)

# https://ablis.business.gov.au/service/nsw/mining-lease/16580
@dataclass
class MiningPurposeLease:
    """
    For anything matching the grammar "Mining Purpose Lease \w+"
    """
    id: str
    
@dataclass
class MineralClaim:
    id: str  

@dataclass
class MineralLease:
    id: str  
    part: Optional[bool] = field(default=False)

@dataclass
class WesternLandLease:
    id: str
    
@dataclass
class RailwayLandLease:
    id: str

@dataclass
class RailcorpFile:
    """
    For anything matching the grammar "RAILCORP. FILE: \w+"
    """
    id: str

# https://www.lls.nsw.gov.au/help-and-advice/travelling-stock-reserves/stock-watering-place-leases
@dataclass
class StockWaterPlaceLease:
    id: str

@dataclass
class PermissiveOccupancy:
    """
    For parising anything with the grammar "Permissive Occupancy \w+"
    """
    id: str

@dataclass
class OccupancyPermit:
    """
    For parsing the grammar "Occupancy Permit \w+"
    """
    id: str

@dataclass
class LeaseNumber:
    """
    For parsing anything that has the grammar "Lease Number \w+( - \w+)"
    """
    id_a: str
    id_b: Optional[str]

@dataclass
class ForestPermit:
    id: str
    
@dataclass
class EnclosurePermit:
    id: str

@dataclass
class DomesticWaterfrontOccupation:
    id: str

@dataclass
class NonIrrigablePurchase:
    id: str

@dataclass
class StateHeritageRegister:
    id: str

@dataclass
class HousingPRN:
    """
    This is for the grammar "Housing PRN \w+"

    I am unsure what this actually is.
    """
    id: str

@dataclass
class CrownLandLicense:
    """
    This is presumably a license to use crown land, the grammar is "Licence \w+"

    Very likely this:
    - https://ablis.business.gov.au/service/nsw/general-licence/40950?locations=NSW
    - https://www.crownland.nsw.gov.au/licences-leases-and-permits/do-i-need-licence-or-lease
    """
    id: str

@dataclass
class CrownReserve:
    """
    For parsing the grammar "Crown Reserve \w+"
    """
    id: str

@dataclass
class CrownPlan:
    """
    For parsing the grammar "Crown Reserve \w+"
    """
    id_a: str
    id_b: str
    part: bool

# https://www.jstor.org/stable/26875607
@dataclass
class SpecialLease:
    """
    For parsing the grammar of "Special Lease \w+"
    """
    id: str

# https://www.transport.nsw.gov.au/operations/roads-and-waterways/waterways/property-planning/maritime-development/maritime
@dataclass
class NswMaritime:
    id: str

@dataclass
class IsDrainageReserve:
    pass

ParsedItem = Union[
    LandParcel,
    WindFarm,
    MineralClaim,
    MineralLease,
    WesternLandLease,
    RailwayLandLease,
    ForestPermit,
    EnclosurePermit,
    IsDrainageReserve,
]

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
    IdPattern(re=re.compile(r'Wind Farm\s+(\w+)'), Const=WindFarm),
    IdPattern(re=re.compile(r"Consolidated Mining Lease\s+(\w+)"), Const=ConsolidatedMiningLease),

    
    IdPattern(re=re.compile(r'Public Reserve\s+(\w+)'), Const=PublicReserve),
    
    # there doesn't appear to be such a thing as a mining permit
    #
    # https://ablis.business.gov.au/search/customsearch
    IdPattern(re=re.compile(r'Mining Permit\s+(\w+)'), Const=MiningLease),
    IdPattern(re=re.compile(r'Telstra Site Number\s+(\w+)'), Const=TelstraSite),
    IdPattern(re=re.compile(r'Mineral Claim\s+(\w+)'), Const=MineralClaim),
    IdPattern(re=re.compile(r'Western Land Lease\s+(\w+)'), Const=WesternLandLease),
    IdPattern(re=re.compile(r'Railway Land Lease\s+(\w+(\.\w+)?)'), Const=RailwayLandLease),
    IdPattern(re=re.compile(r'Stock Watering Place\s+(\w+)'), Const=StockWaterPlaceLease),
    IdPattern(re=re.compile(r'Special Lease\s+(\w+)'), Const=SpecialLease),
    IdPattern(re=re.compile(r'Forest Permit\s+(\w+)'), Const=ForestPermit),
    IdPattern(re=re.compile(r'Enclosure Permit\s+(\w+)'), Const=EnclosurePermit),
    IdPattern(re=re.compile(r'Non-Irrigable Purchase\s+(\w+)'), Const=NonIrrigablePurchase),
    IdPattern(re=re.compile(r'NSW Maritime\s+(\w+)'), Const=NswMaritime),
    IdPattern(re=re.compile(r'Housing PRN\s+(\w+)'), Const=HousingPRN),
    IdPattern(re=re.compile(r'Licence\s+(\w+)'), Const=CrownLandLicense),
    IdPattern(re=re.compile(r'BUS DEPOT LEASE\s+(\w+)'), Const=BusDepotLease),
    
    # IdPattern(re=re.compile(r'STATE HERITAGE REGISTRAR\s+(\w+)', re.IGNORECASE), Const=StateHeritageRegister),
    IdPattern(re=re.compile(r'State Heritage Listing No\s+(\w+)', re.IGNORECASE), Const=StateHeritageRegister),
    IdPattern(re=re.compile(r'Permissive Occupancy\s+(\w+)'), Const=PermissiveOccupancy),
    IdPattern(re=re.compile(r'RAILCORP\. FILE:\s+(\w+)', re.IGNORECASE), Const=RailcorpFile),
    IdPattern(re=re.compile(r'Domestic Waterfront Occupancy\s+(\w+)'), Const=DomesticWaterfrontOccupation),
    
    # I am guessing these are the same things
    IdPattern(re=re.compile(r'Occupation Permit PB\s+(\w+)'), Const=OccupancyPermit),
    IdPattern(re=re.compile(r'Occupation Permit\s+(\w+)'), Const=OccupancyPermit),
    IdPattern(re=re.compile(r'Occupancy Permit\s+(\w+)'), Const=OccupancyPermit),
]

named_group_patterns = [
    NamePattern(
        re=re.compile(r'State Heritage\s+(Register|REGISTRAR)\s+(SHR\s+NO\.\s+)?(?P<id>\w+)', re.IGNORECASE),
        id_names=['id'],
        Const=StateHeritageRegister,
    ),
    NamePattern(
        re=re.compile(r'Subject to (SHR|SHRL) No\s+(?P<id>\w+)', re.IGNORECASE),
        id_names=['id'],
        Const=StateHeritageRegister,
    ),
    NamePattern(
        re=re.compile(r'Crown Reserve\s+(?P<id>\w+)'),
        id_names=['id'],
        Const=CrownReserve,
    ),
    NamePattern(
        re=re.compile(r'Part Crown Plan\s+(?P<id_a>\w+)-(?P<id_b>\w+)'),
        id_names=['id_a', 'id_b'],
        Const=lambda **kwargs: CrownPlan(**kwargs, part=True),
    ),
    NamePattern(
        re=re.compile(r'Crown Plan\s+(?P<id_a>\w+)-(?P<id_b>\w+)(?P<part>\s+\(Part\))?'),
        id_names=['id_a', 'id_b'],
        bool_names=['part'],
        Const=CrownPlan,
    ),
    # Mining Lease — You will need a mining lease to extract minerals for the PURPOSE of commercial mining
    # via: https://ablis.business.gov.au/service/nsw/mining-lease/16580
    NamePattern(
        re=re.compile(r'Mining (Purpose )?Lease\s+(?P<id>\w+)(?P<part>\s+\(Part\))?', re.IGNORECASE), 
        id_names=['id'],
        bool_names=['part'],
        Const=MiningLease,
    ),
    NamePattern(
        re=re.compile(r"Consolidated Coal Lease\s+(?P<id>\w+)(?P<part>\s+\(Part\))?", re.IGNORECASE), 
        id_names=['id'],
        bool_names=['part'],
        Const=ConsolidatedCoalLease,
    ),
    NamePattern(
        re=re.compile(r'Coal Lease\s+(?P<id>\w+)(?P<part>\s+\(Part\))?', re.IGNORECASE), 
        id_names=['id'],
        bool_names=['part'],
        Const=CoalLease,
    ),
    NamePattern(
        re=re.compile(r'Mineral Lease\s+(?P<id>\w+)(?P<part>\s+\(Part\))?', re.IGNORECASE), 
        id_names=['id'],
        bool_names=['part'],
        Const=MineralLease,
    ),
    NamePattern(
        re=re.compile(r'Lease Number\s+(?P<id_a>\w+) TO (?P<id_b>\w+)'),
        id_names=['id_a', 'id_b'],
        Const=LeaseNumber,
    ),
    NamePattern(
        re=re.compile(r'Lease Number\s+(?P<id_a>\w+)( - (?P<id_b>\w+))?'),
        id_names=['id_a', 'id_b'],
        Const=LeaseNumber,
    ),
    NamePattern(
        re=re.compile(r'Site\s+(?P<site>\w+) of Sydney Ports Corporation Plan\s+(?P<plan>\w+)'),
        id_names=['site', 'plan'],
        Const=SydneyPortsCorporationPlan,
    ),
]

flag_patterns = [
    FlagPattern(re=re.compile('DRAINAGE RESERVE'), Const=IsDrainageReserve),
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
    
    # only appears on 3 different properties
    re.compile(r'Share Use', re.IGNORECASE),
    re.compile(r'Shared Use', re.IGNORECASE), 

    # I saw this stand alone after a parcel number with zero context.
    re.compile(r'floor space area'), 
    
    # idk what's going with this...
    # it's often trailing after licenses of permits
    # re.compile(r'(PART)'), 
]

def parse_land_parcel_ids(desc: str):
    def read_chunk(read_from, skip = 0):
        copy = desc[read_from:]
        while skip > 0:
            copy = copy[copy.find(' ') + 1:]
            skip -= 1
        if copy.find(' ') == -1:
            return copy
        else:
            return copy[:copy.find(' ')]
        
    def move_cursor(read_from, skip = 0):
        while skip > 0:
            if desc[read_from:].find(' ') == -1:
                return len(desc)
                
            read_from = read_from + desc[read_from:].find(' ') + 1
            skip -= 1
        return read_from
        
    def impl():
        read_from = 0
        chunk = None
        
        while read_from < len(desc):
            chunk = read_chunk(read_from, skip=0)
            # print(read_from, desc[read_from:], chunk, f"'{read_chunk(read_from, skip=1)}'")
            
            if '/' in chunk:
                yield LandParcel(id=chunk, part=False)
                read_from = move_cursor(read_from, 1)
                continue
                
            if 'PT' == chunk and '/' in read_chunk(read_from, skip=1):
                yield LandParcel(id=read_chunk(read_from, skip=1), part=True)
                read_from = move_cursor(read_from, 2)
                continue

            if 'PT' != chunk and not chunk.endswith(','):
                return desc[read_from:] 

            lots = []
            plan = ''
            while True:                    
                chunk = read_chunk(read_from)
                part = False
                
                if chunk == 'PT':
                    part = True
                    read_from = move_cursor(read_from, 1)
                    chunk = read_chunk(read_from)
                    
                if chunk.endswith(','):
                    lots.append((part, chunk[:-1]))
                    read_from = move_cursor(read_from, 1)
                    continue  
                elif '/' in chunk:
                    lots.append((part, chunk[:chunk.find('/')]))
                    plan = chunk[chunk.find('/'):]
                    read_from = move_cursor(read_from, 1)
                    break
                else:
                    return desc[read_from:]
                    
            for part, lot in lots:
                yield LandParcel(id=f'{lot}{plan}', part=part)
        return desc[read_from:]
    
    land_parcels: List[LandParcel] = []
    gen = impl()
    
    try:
        while True:
            land_parcels.append(next(gen))
    except StopIteration as e:
        return e.value, land_parcels

def parse_property_description(description: str) -> List[ParsedItem]:
    description = re.sub(r'\s+', ' ', description)
    parsed_items: List[ParsedItem] = []

    for pattern in ignore_pre_patterns:
        description = pattern.sub('', description)

    for pattern in id_patterns:
        for match in pattern.re.finditer(description):
            parsed_items.append(pattern.Const(id=match.group(1)))
        description = pattern.re.sub('', description)

    for pattern in named_group_patterns:
        for match in pattern.re.finditer(description):
            parsed_item = pattern.Const(
                **{ k: match.group(k) for k in pattern.id_names },
                **{ 
                    k: match.group(k) is not None
                    for k in pattern.bool_names
                },
            )
            parsed_items.append(parsed_item)
        description = pattern.re.sub('', description)

    for pattern in flag_patterns:
        for match in pattern.re.finditer(description):
            parsed_items.append(pattern.Const())
        description = pattern.re.sub('', description)

    for pattern in ignore_post_patterns:
        description = pattern.sub('', description)
        
    description = re.sub(r'\s+', ' ', description)
    description, land_parcels = parse_land_parcel_ids(description)
    description = re.sub(r'(\s+|\.)+', '', description)
    parsed_items.extend(land_parcels)
    return description, parsed_items
