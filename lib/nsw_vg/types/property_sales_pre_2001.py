from dataclasses import dataclass
from datetime import datetime
from typing import Optional

def parse_datetime(date_str):
    return datetime.strptime(date_str, "%Y%m%d %H:%M")
    
def parse_date(date_str):
    return datetime.strptime(date_str, "%d/%m/%Y")

@dataclass
class SaleRecordFile:
    district: Optional[int]
    submitting_user_id: Optional[str]
    date_downloaded: datetime
    file_path: str

    @staticmethod
    def from_row(row, file_path=None, **kwargs):
        return SaleRecordFile(
            file_path=file_path,
            district=row[0] and int(row[0]),
            submitting_user_id=row[1],
            date_downloaded=parse_datetime(row[2]),
        )

@dataclass
class SalePropertyDetails:
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
    def from_row(row):
        return SalePropertyDetails(
            district=int(row[0]),
            source=row[1],
            valuation_num=row[2],
            property_id=row[3],
            property_unit_number=row[4] or None,
            property_house_number=row[5] or None,
            property_street_name=row[6] or None,
            property_locality=row[7] or None,
            property_postcode=int(row[8]) if row[8] else None,
            contract_date=parse_date(row[9]),
            purchase_price=float(row[10]),
            land_description=row[11],
            area=float(row[12]) if row[12] else None,
            area_type=row[13] or None,
            dimensions=row[14],
            zoning=row[15] or None,
            comp_code=row[16] or None,
        )

@dataclass
class SaleDataFileSummary:
    total_records: int
    total_sale_property_details: int

    @staticmethod
    def from_row(row):
        return SaleDataFileSummary(
            total_records=int(row[0]),
            total_sale_property_details=int(row[1]),
        )
