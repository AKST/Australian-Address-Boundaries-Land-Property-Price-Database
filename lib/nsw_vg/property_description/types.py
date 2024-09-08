from dataclasses import dataclass, field
from collections import namedtuple
from typing import List, Union, Any, Optional

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
class PerpetualLease:
    """
    For parsing anything that has the grammar "Perpetual Lease \w+"
    """
    id: str

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