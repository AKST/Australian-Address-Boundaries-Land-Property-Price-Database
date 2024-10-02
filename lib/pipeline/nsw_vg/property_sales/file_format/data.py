import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Self, Literal

from lib.pipeline.nsw_vg.discovery import NswVgTarget

@dataclass
class PropertySaleDatFileMetaData:
    file_path: str
    published_year: int
    download_date: Optional[datetime]
    size: int

class BasePropertySaleFileRow:
    pass

@dataclass
class SaleRecordFile(BasePropertySaleFileRow):
    year_of_sale: int = field(repr=False)
    file_path: str = field(repr=False)
    file_type: Optional[str]
    date_provided: datetime
    submitting_user_id: Optional[str]
    district_code: int

@dataclass
class SaleRecordFileLegacy(BasePropertySaleFileRow):
    year_of_sale: int = field(repr=False)
    file_path: str = field(repr=False)
    date_provided: datetime
    submitting_user_id: Optional[str]

@dataclass
class SalePropertyDetailsCommon:
    property_id: Optional[int]
    # TODO confirm this is optional
    contract_date: Optional[datetime]
    """
    From 2002 onwards always defined.
    """
    purchase_price: Optional[float]
    district_code: int
    address: 'Address'
    area: Optional['Area']
    zoning: Optional['AbstractZoning']
    comp_code: Optional[str]

@dataclass
class SalePropertyDetails(BasePropertySaleFileRow):
    parent: SaleRecordFile = field(repr=False)
    common: SalePropertyDetailsCommon
    sale_counter: int
    date_provided: datetime
    settlement_date: Optional[datetime]
    nature_of_property: str
    primary_purpose: Optional[str]
    strata_lot_number: Optional[int]
    sale_code: Optional[str]
    interest_of_sale: Optional[int]
    dealing_number: str

@dataclass
class SalePropertyDetails1990(BasePropertySaleFileRow):
    parent: SaleRecordFile = field(repr=False)
    common: SalePropertyDetailsCommon
    source: Optional[str]
    valuation_num: Optional[str]
    land_description: Optional[str]
    dimensions: Optional[str]

@dataclass
class SalePropertyLegalDescription(BasePropertySaleFileRow):
    parent: SalePropertyDetails = field(repr=False)

    district_code: int

    """
    Missing in property sale records from July 2001
    """
    property_id: Optional[int]
    sale_counter: int
    date_provided: datetime
    property_description: Optional[str]

@dataclass
class SaleParticipant(BasePropertySaleFileRow):
    parent: SalePropertyLegalDescription = field(repr=False)
    district_code: int
    """
    Missing in property sale records from July 2001
    """
    property_id: Optional[int]
    sale_counter: int
    date_provided: datetime
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
    unit: Optional[str]
    amount: float
    stardard: bool = field(default=True)

@dataclass
class Address:
    property_name: Optional[str]
    unit_number: Optional[str]
    house_number: Optional[str]
    street_name: Optional[str]
    locality: Optional[str]
    postcode: Optional[int]

ZoneKind = Literal['legacy', '2011.08']

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
        return '2011.08'

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
        return 'legacy'


