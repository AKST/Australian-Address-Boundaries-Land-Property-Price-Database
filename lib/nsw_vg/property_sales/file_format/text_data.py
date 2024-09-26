from dataclasses import dataclass
from datetime import datetime
from typing import Optional

class BasePropertySaleFileRow:
    pass

@dataclass
class SaleRecordFile(BasePropertySaleFileRow):
    year: int
    district: int
    file_path: str
    file_type: Optional[str]
    date_downloaded: datetime
    submitting_user_id: Optional[str]

@dataclass
class SaleRecordFile1990(BasePropertySaleFileRow):
    year: int
    district: Optional[int]
    submitting_user_id: Optional[str]
    date_downloaded: datetime
    file_path: str

@dataclass
class SalePropertyDetails(BasePropertySaleFileRow):
    parent: SaleRecordFile

    district: int
    property_id: str
    sale_counter: str
    date_downloaded: datetime
    property_name: Optional[str]
    property_unit_number: Optional[str]
    property_house_number: Optional[str]
    property_street_name: Optional[str]
    property_locality: Optional[str]
    property_postcode: Optional[int]
    area: Optional[float]
    area_type: Optional[str]

    # TODO confirm this is optional
    contract_date: Optional[datetime]

    # TODO confirm this is optional
    settlement_date: Optional[datetime]

    """
    From 2002 onwards always defined.
    """
    purchase_price: Optional[float]
    zoning: Optional[str]
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
    parent: SaleRecordFile1990

    district: int
    source: Optional[str]
    valuation_num: str
    property_id: str
    property_unit_number: Optional[str]
    property_house_number: Optional[str]
    property_street_name: Optional[str]
    property_locality: Optional[str]
    property_postcode: Optional[int]
    contract_date: datetime
    purchase_price: float
    land_description: str
    area: Optional[float]
    area_type: Optional[str]
    dimensions: str
    zoning: Optional[str]
    comp_code: Optional[str]

@dataclass
class SalePropertyLegalDescription(BasePropertySaleFileRow):
    parent: SalePropertyDetails

    district: int
    property_id: str
    sale_counter: str
    date_downloaded: datetime
    property_legal_description: str

@dataclass
class SaleParticipant(BasePropertySaleFileRow):
    parent: SalePropertyLegalDescription
    district: int
    property_id: str
    sale_counter: str
    date_downloaded: datetime
    participant: str

@dataclass
class SaleDataFileSummary(BasePropertySaleFileRow):
    parent: SaleRecordFile
    total_records: int
    total_sale_property_details: int

    # fields not provided in 1990 format
    total_sale_property_legal_descriptions: int

    # fields not provided in 1990 format
    total_sale_participants: int
