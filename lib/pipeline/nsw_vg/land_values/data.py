from dataclasses import dataclass, fields
from datetime import datetime
from typing import Callable, Dict, List, TypeVar, Optional

from lib.pipeline.nsw_vg.raw_data.rows import *
from lib.pipeline.nsw_vg.raw_data.zoning import ZoningKind

@dataclass
class RawLandValueRow:
    district_code: int
    district_name: Optional[str]
    property_id: int
    property_type: str
    property_name: Optional[str]
    unit_number: Optional[str]
    house_number: Optional[str]
    street_name: Optional[str]
    suburb_name: str
    postcode: Optional[str]
    property_description: Optional[str]
    zone_code: Optional[str]
    zone_standard: ZoningKind | None
    area: Optional[float]
    area_type: Optional[str]
    land_value_1: Optional[float]
    base_date_1: Optional[datetime]
    authority_1: Optional[str]
    basis_1: Optional[str]
    land_value_2: Optional[float]
    base_date_2: Optional[datetime]
    authority_2: Optional[str]
    basis_2: Optional[str]
    land_value_3: Optional[float]
    base_date_3: Optional[datetime]
    authority_3: Optional[str]
    basis_3: Optional[str]
    land_value_4: Optional[float]
    base_date_4: Optional[datetime]
    authority_4: Optional[str]
    basis_4: Optional[str]
    land_value_5: Optional[float]
    base_date_5: Optional[datetime]
    authority_5: Optional[str]
    basis_5: Optional[str]

    source_file_name: str
    source_line_number: int
    source_date: datetime

    @staticmethod
    def from_row(row: Dict[str, str],
                 line_number: int,
                 file_path: str,
                 source_date: datetime) -> 'RawLandValueRow':
        return RawLandValueRow(
            district_code=read_int(row, 'DISTRICT CODE', 'district_code'),
            district_name=row['DISTRICT NAME'] or None,
            property_id=read_int(row, 'PROPERTY ID', 'property_id'),
            property_type=read_str(row, 'PROPERTY TYPE', 'property_type'),
            property_name=row['PROPERTY NAME'] or None,
            unit_number=row['UNIT NUMBER'] or None,
            house_number=row['HOUSE NUMBER'] or None,
            street_name=row['STREET NAME'] or None,
            suburb_name=read_str(row, 'SUBURB NAME', 'suburb_name'),
            postcode=StrCheck(max_len=4).read_optional(row, 'POSTCODE', 'postcode'),
            property_description=row['PROPERTY DESCRIPTION'] or None,
            zone_code=StrCheck(max_len=4).read_optional(row, 'ZONE CODE', 'zone_code'),
            zone_standard=read_zone_std(row, 'ZONE CODE', 'zone_standard'),
            area=read_optional_float(row, 'AREA', 'area'),
            area_type=read_area_type(row, 'AREA TYPE', 'area_type'),
            land_value_1=read_optional_float(row, 'LAND VALUE 1', 'land_value_1'),
            base_date_1=read_optional_date_pre_2002(row, 'BASE DATE 1', 'base_date_1'),
            authority_1=row['AUTHORITY 1'] or None,
            basis_1=row['BASIS 1'] or None,
            land_value_2=read_optional_float(row, 'LAND VALUE 2', 'land_value_2'),
            base_date_2=read_optional_date_pre_2002(row, 'BASE DATE 2', 'base_date_2'),
            authority_2=row['AUTHORITY 2'] or None,
            basis_2=row['BASIS 2'] or None,
            land_value_3=read_optional_float(row, 'LAND VALUE 3', 'land_value_3'),
            base_date_3=read_optional_date_pre_2002(row, 'BASE DATE 3', 'base_date_3'),
            authority_3=row['AUTHORITY 3'] or None,
            basis_3=row['BASIS 3'] or None,
            land_value_4=read_optional_float(row, 'LAND VALUE 4', 'land_value_4'),
            base_date_4=read_optional_date_pre_2002(row, 'BASE DATE 4', 'base_date_4'),
            authority_4=row['AUTHORITY 4'] or None,
            basis_4=row['BASIS 4'] or None,
            land_value_5=read_optional_float(row, 'LAND VALUE 5', 'land_value_5'),
            base_date_5=read_optional_date_pre_2002(row, 'BASE DATE 5', 'base_date_5'),
            authority_5=row['AUTHORITY 5'] or None,
            basis_5=row['BASIS 5'] or None,
            source_file_name=file_path,
            source_line_number=line_number,
            source_date=source_date,
        )

    def db_columns(self) -> List[str]:
        return [f.name for f in fields(self)]
