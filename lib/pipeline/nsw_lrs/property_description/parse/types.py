from dataclasses import dataclass, field
from collections import namedtuple
from typing import List, Union, Any, Optional

class ParseItem:
    pass

@dataclass
class LandParcel(ParseItem):
    id: str
    part: bool = field(default=False)

PublicReserve = namedtuple("PublicReserve", ['id'])
BusDepotLease = namedtuple("BusDepotLease", ['id'])
WindFarm = namedtuple("WindFarm", ['id'])
TelstraSite = namedtuple("TelstraSite", ['id'])
SydneyPortsCorporationPlan = namedtuple('SydneyPortsCorporationPlan', ['site', 'plan'])

@dataclass
class CoalLease(ParseItem):
    """
    For anything matching the grammar "Coal Lease \w+"
    """
    id: str
    part: Optional[bool] = field(default=False)

@dataclass
class ConsolidatedCoalLease(ParseItem):
    """
    For anything matching the grammar "Consolidated Coal Lease \w+"
    """
    id: str
    part: bool

@dataclass
class ConsolidatedMiningLease(ParseItem):
    """
    For anythign matching the grammar "Consolidated Mining Lease \w+"
    """
    id: str

# https://ablis.business.gov.au/service/nsw/mining-lease/16580
@dataclass
class MiningLease(ParseItem):
    """
    For anything matching the grammar "Mining Lease \w+"
    """
    id: str
    part: Optional[bool] = field(default=False)

# https://ablis.business.gov.au/service/nsw/mining-lease/16580
@dataclass
class MiningPurposeLease(ParseItem):
    """
    For anything matching the grammar "Mining Purpose Lease \w+"
    """
    id: str

@dataclass
class MineralClaim(ParseItem):
    id: str

@dataclass
class MineralLease(ParseItem):
    id: str
    part: Optional[bool] = field(default=False)

@dataclass
class WesternLandLease(ParseItem):
    id: str

@dataclass
class RailwayLandLease(ParseItem):
    id: str

@dataclass
class RailcorpFile(ParseItem):
    """
    For anything matching the grammar "RAILCORP. FILE: \w+"
    """
    id: str

# https://www.lls.nsw.gov.au/help-and-advice/travelling-stock-reserves/stock-watering-place-leases
@dataclass
class StockWaterPlaceLease(ParseItem):
    id: str

@dataclass
class PermissiveOccupancy(ParseItem):
    """
    For parising anything with the grammar "Permissive Occupancy \w+"
    """
    id: str

@dataclass
class OccupancyPermit(ParseItem):
    """
    For parsing the grammar "Occupancy Permit \w+"
    """
    id: str

@dataclass
class LeaseNumber(ParseItem):
    """
    For parsing anything that has the grammar "Lease Number \w+( - \w+)"
    """
    id_a: str
    id_b: Optional[str]

@dataclass
class PerpetualLease(ParseItem):
    """
    For parsing anything that has the grammar "Perpetual Lease \w+"
    """
    id: str

@dataclass
class ForestPermit(ParseItem):
    id: str

@dataclass
class EnclosurePermit(ParseItem):
    id: str

@dataclass
class DomesticWaterfrontOccupation(ParseItem):
    id: str

@dataclass
class NonIrrigablePurchase(ParseItem):
    id: str

@dataclass
class StateHeritageRegister(ParseItem):
    id: str

@dataclass
class HousingPRN(ParseItem):
    """
    This is for the grammar "Housing PRN \w+"

    I am unsure what this actually is.
    """
    id: str

@dataclass
class CrownLandLicense(ParseItem):
    """
    This is presumably a license to use crown land, the grammar is "Licence \w+"

    Very likely this:
    - https://ablis.business.gov.au/service/nsw/general-licence/40950?locations=NSW
    - https://www.crownland.nsw.gov.au/licences-leases-and-permits/do-i-need-licence-or-lease
    """
    id: str

@dataclass
class CrownReserve(ParseItem):
    """
    For parsing the grammar "Crown Reserve \w+"
    """
    id: str

@dataclass
class CrownPlan(ParseItem):
    """
    For parsing the grammar "Crown Reserve \w+"
    """
    id_a: str
    id_b: str
    part: bool

# https://www.jstor.org/stable/26875607
@dataclass
class SpecialLease(ParseItem):
    """
    For parsing the grammar of "Special Lease \w+"
    """
    id: str

# https://www.transport.nsw.gov.au/operations/roads-and-waterways/waterways/property-planning/maritime-development/maritime
@dataclass
class NswMaritime(ParseItem):
    id: str

@dataclass
class IsDrainageReserve(ParseItem):
    pass

