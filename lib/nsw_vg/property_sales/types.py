from dataclasses import dataclass
from datetime import datetime
from typing import Optional

def parse_datetime(date_str):
    return datetime.strptime(date_str, "%Y%m%d %H:%M")

def parse_date(date_str):
    return datetime.strptime(date_str, "%Y%m%d")

def parse_date_pre_2002(date_str):
    return datetime.strptime(date_str, "%d/%m/%Y")

@dataclass
class SaleRecordFile:
    year: int
    district: int
    file_path: str
    file_type: Optional[str]
    date_downloaded: datetime
    submitting_user_id: Optional[str]

    @staticmethod
    def from_row(row, file_path=None, year=None, **kwargs):
        return SaleRecordFile(
            year=year,
            file_path=file_path,
            file_type=row[0],
            district=int(row[1]),
            date_downloaded=parse_datetime(row[2]),
            submitting_user_id=row[3],
        )

@dataclass
class SaleRecordFile2002:
    year: int
    district: int
    file_path: str
    date_downloaded: datetime
    submitting_user_id: Optional[str]

    @staticmethod
    def from_row(row, file_path=None, year=None, **kwargs):
        return SaleRecordFile2002(
            year=year,
            file_path=file_path,
            district=int(row[0]),
            date_downloaded=parse_datetime(row[1]),
            submitting_user_id=row[2],
        )

@dataclass
class SaleRecordFile1990:
    year: int
    district: Optional[int]
    submitting_user_id: Optional[str]
    date_downloaded: datetime
    file_path: str

    @staticmethod
    def from_row(row, file_path=None, year=None, **kwargs):
        return SaleRecordFile1990(
            year=year,
            file_path=file_path,
            district=row[0] and int(row[0]),
            submitting_user_id=row[1],
            date_downloaded=parse_datetime(row[2]),
        )

@dataclass
class SalePropertyDetails:
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
    contract_date: datetime
    settlement_date: datetime
    """
    From 2002 onwards always defined.
    """
    purchase_price: float
    zoning: Optional[str]
    nature_of_property: Optional[int]
    primary_purpose: Optional[int]
    strata_lot_number: Optional[int]
    component_code: Optional[int]
    sale_code: Optional[int]
    interest_of_sale: Optional[int]
    dealing_number: str

    @staticmethod
    def from_row(row, parent):
        return SalePropertyDetails(
            parent=parent,
            district=int(row[0]),
            property_id=row[1],
            sale_counter=row[2],
            date_downloaded=parse_datetime(row[3]),
            property_name=row[4] or None,
            property_unit_number=row[5] or None,
            property_house_number=row[6] or None,
            property_street_name=row[7] or None,
            property_locality=row[8] or None,
            property_postcode=int(row[9]) if row[9] else None,
            area=float(row[10]) if row[10] else None,
            area_type=row[11] or None,
            contract_date=parse_date(row[12]) if row[12] else None,
            settlement_date=parse_date(row[13]) if row[13] else None,
            purchase_price=float(row[14]) if row[14] else None,
            zoning=row[15] or None,
            nature_of_property=row[16] or None,
            primary_purpose=row[17] or None,
            strata_lot_number=row[18] or None,
            component_code=row[19] or None,
            sale_code=row[20] or None,
            interest_of_sale=row[21] or None,
            dealing_number=row[22],
        )

@dataclass
class SalePropertyDetails1990:
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
    comp_code: Optional[int]

    @staticmethod
    def from_row(row, parent):
        return SalePropertyDetails1990(
            parent=parent,
            district=int(row[0]),
            source=row[1],
            valuation_num=row[2],
            property_id=row[3],
            property_unit_number=row[4] or None,
            property_house_number=row[5] or None,
            property_street_name=row[6] or None,
            property_locality=row[7] or None,
            property_postcode=int(row[8]) if row[8] else None,
            contract_date=parse_date_pre_2002(row[9]),
            purchase_price=float(row[10]),
            land_description=row[11],
            area=float(row[12]) if row[12] else None,
            area_type=row[13] or None,
            dimensions=row[14],
            zoning=row[15] or None,
            comp_code=row[16] or None,
        )

@dataclass
class SalePropertyLegalDescription:
    parent: SalePropertyDetails

    district: int
    property_id: str
    sale_counter: str
    date_downloaded: datetime
    property_legal_description: str

    @staticmethod
    def from_row(row, parent):
        return SalePropertyLegalDescription(
            parent=parent,
            district=int(row[0]),
            property_id=row[1],
            sale_counter=row[2],
            date_downloaded=parse_datetime(row[3]),
            property_legal_description=row[4],
        )

@dataclass
class SaleParticipant:
    parent: SalePropertyLegalDescription
    district: int
    property_id: str
    sale_counter: str
    date_downloaded: datetime
    participant: str

    @staticmethod
    def from_row(row, parent):
        return SaleParticipant(
            parent=parent,
            district=int(row[0]),
            property_id=row[1],
            sale_counter=row[2],
            date_downloaded=parse_datetime(row[3]),
            participant=row[4],
        )

@dataclass
class SaleDataFileSummary:
    parent: SaleRecordFile
    total_records: int
    total_sale_property_details: int
    total_sale_property_legal_descriptions: int
    total_sale_participants: int

    @staticmethod
    def from_row(row, parent):
        return SaleDataFileSummary(
            parent=parent,
            total_records=int(row[0]),
            total_sale_property_details=int(row[1]),
            total_sale_property_legal_descriptions=int(row[2]),
            total_sale_participants=int(row[3]),
        )

@dataclass
class SaleDataFileSummary1990:
    parent: SaleRecordFile1990
    total_records: int
    total_sale_property_details: int

    @staticmethod
    def from_row(row, parent):
        return SaleDataFileSummary1990(
            parent=parent,
            total_records=int(row[0]),
            total_sale_property_details=int(row[1]),
        )

t_current = {
    'A': SaleRecordFile,
    'B': SalePropertyDetails,
    'C': SalePropertyLegalDescription,
    'D': SaleParticipant,
    'Z': SaleDataFileSummary,
}

t_2002 = {
    'A': SaleRecordFile2002,
    'B': SalePropertyDetails,
    'C': SalePropertyLegalDescription,
    'D': SaleParticipant,
    'Z': SaleDataFileSummary,
}

t_1990 = {
    'A': SaleRecordFile1990,
    'B': SalePropertyDetails1990,
    'Z': SaleDataFileSummary1990,
}
