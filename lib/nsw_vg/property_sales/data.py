import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Self, Literal

class BasePropertySaleFileRow:
    pass

@dataclass
class SaleRecordFile(BasePropertySaleFileRow):
    year: int = field(repr=False)
    file_path: str = field(repr=False)
    file_type: Optional[str]
    date_downloaded: datetime
    submitting_user_id: Optional[str]
    """
    Missing in some data published prior to 2002
    """
    district: Optional[int]

@dataclass
class SalePropertyDetails(BasePropertySaleFileRow):
    parent: SaleRecordFile = field(repr=False)

    district: int
    property_id: str
    sale_counter: int
    date_downloaded: datetime = field(repr=False)
    property_name: Optional[str]
    property_unit_number: Optional[str]
    property_house_number: Optional[str]
    property_street_name: Optional[str]
    property_locality: Optional[str]
    property_postcode: Optional[int]
    area: Optional['Area']

    # TODO confirm this is optional
    contract_date: Optional[datetime] = field(repr=False)

    # TODO confirm this is optional
    settlement_date: Optional[datetime] = field(repr=False)

    """
    From 2002 onwards always defined.
    """
    purchase_price: Optional[float]
    zoning: Optional['AbstractZoning']
    nature_of_property: Optional[str]
    primary_purpose: Optional[str]

    # TODO some of these may may be ints.
    strata_lot_number: Optional[str]
    component_code: Optional[str]
    sale_code: Optional[str]
    interest_of_sale: Optional[str]

    dealing_number: str

@dataclass
class SalePropertyDetails1990(BasePropertySaleFileRow):
    parent: SaleRecordFile = field(repr=False)

    district: int
    source: Optional[str]
    valuation_num: str
    property_id: str
    property_unit_number: Optional[str]
    property_house_number: Optional[str]
    property_street_name: Optional[str]
    property_locality: Optional[str]
    property_postcode: Optional[int]
    contract_date: datetime = field(repr=False)
    purchase_price: float
    land_description: str
    area: Optional['Area']
    dimensions: Optional[str]
    comp_code: Optional[str]
    zoning: Optional['AbstractZoning']

@dataclass
class SalePropertyLegalDescription(BasePropertySaleFileRow):
    parent: SalePropertyDetails = field(repr=False)

    district: int

    """
    Missing in property sale records from July 2001
    """
    property_id: Optional[int]
    sale_counter: str
    date_downloaded: datetime = field(repr=False)
    property_legal_description: str

@dataclass
class SaleParticipant(BasePropertySaleFileRow):
    parent: SalePropertyLegalDescription = field(repr=False)
    district: int
    """
    Missing in property sale records from July 2001
    """
    property_id: Optional[int]
    sale_counter: str
    date_downloaded: datetime = field(repr=False)
    participant: str

@dataclass
class SaleDataFileSummary(BasePropertySaleFileRow):
    parent: SaleRecordFile = field(repr=False)
    total_records: int
    total_sale_property_details: int

    # fields not provided in 1990 format
    total_sale_property_legal_descriptions: int

    # fields not provided in 1990 format
    total_sale_participants: int

@dataclass
class Area:
    unit: str
    amount: float

@dataclass
class SalePropertyLegacyData:
    source: Optional[str]
    valuation_num: str
    comp_code: Optional[str]


ZoneKind = Literal['< 2011', '>= 2011']

class AbstractZoning(abc.ABC):
    @property
    @abc.abstractmethod
    def kind(self: Self) -> ZoneKind:
        pass

@dataclass
class ZoningPost2011(AbstractZoning):
    """
    These zones are used in sales information prior to 2011.
    They are the same as the ones introduced in 2006.

    https://legislation.nsw.gov.au/view/pdf/asmade/epi-2006-155

    They were not used by the Register of Land Values until
    2011. Instead they used more general zones.
    """
    zone: str

    @property
    def kind(self: Self) -> ZoneKind:
        return '>= 2011'

@dataclass
class ZoningLegacy(AbstractZoning):
    """
    Prior to 2011, single character zone codes were used
    to classify the zones of properties recorded in the
    Register of Land Values.

    https://www.nsw.gov.au/sites/default/files/noindex/2024-05/Property_Sales_Data_File_Zone_Codes_and_Descriptions_V2.pdf
    """
    zone: str

    @property
    def kind(self: Self) -> ZoneKind:
        return '< 2011'


