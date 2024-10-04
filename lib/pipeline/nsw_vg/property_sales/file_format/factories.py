import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Self, List, TypeVar, Optional

from lib.pipeline.nsw_vg.property_sales.file_format import data as t

class AbstractFormatFactory(abc.ABC):
    @classmethod
    def create(cls, year: int, file_path: str) -> 'AbstractFormatFactory':
        raise NotImplementedError('create not implemented on AbstractFormatFactory')

    @abc.abstractmethod
    def create_a(self: Self, pos: int, row: List[str], variant: Optional[str]) -> t.BasePropertySaleFileRow:
        pass

    @abc.abstractmethod
    def create_b(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]) -> t.BasePropertySaleFileRow:
        pass

    @abc.abstractmethod
    def create_c(self: Self, pos: int, row: List[str], b_record: Any, variant: Optional[str]) -> t.SalePropertyLegalDescription:
        pass

    @abc.abstractmethod
    def create_d(self: Self, pos: int, row: List[str], c_record: Any, variant: Optional[str]) -> t.SaleParticipant:
        pass

    @abc.abstractmethod
    def create_z(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]) -> t.SaleDataFileSummary:
        pass

class CurrentFormatFactory(AbstractFormatFactory):
    zone_standard: t.ZoningKind = 'ep&a_2006'
    zone_code_len: int = 3

    def __init__(self: Self, year: int, file_path: str):
        self.year = year
        self.file_path = file_path

    @classmethod
    def create(Cls, year: int, file_path: str) -> 'CurrentFormatFactory':
        return CurrentFormatFactory(year, file_path)

    def create_a(self: Self, pos: int, row: List[str], variant: Optional[str]):
        return t.SaleRecordFile(
            position=pos,
            year_of_sale=self.year,
            file_path=self.file_path,
            file_type=row[0] or None,
            district_code=read_int(row, 1, 'district_code'),
            date_provided=read_datetime(row, 2, 'date_provided'),
            submitting_user_id=read_str(row, 3, 'submitting_user_id'),
        )

    def create_b(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SalePropertyDetails(
            position=pos,
            parent=a_record,
            district_code=read_int(row, 0, 'district_code'),
            property_id=read_optional_int(row, 1, 'property_id'),
            sale_counter=read_int(row, 2, 'sale_counter'),
            date_provided=read_datetime(row, 3, 'date_provided'),
            property_name=row[4] or None,
            unit_number=row[5] or None,
            house_number=row[6] or None,
            street_name=row[7],
            locality_name=row[8],
            postcode=read_optional_int(row, 9, 'property_postcode'),
            area=read_optional_float(row, 10, 'area'),
            area_type=read_area_type(row, 11, 'area_type'),
            contract_date=read_optional_date(row, 12, 'contract_date'),
            settlement_date=read_optional_date(row, 13, 'settlement_date'),
            purchase_price=read_optional_float(row, 14, 'purchase_price'),
            zone_code=StrCheck(max_len=self.zone_code_len).read_optional(row, 15, 'zone_code'),
            zone_standard=read_zone_std(row, 15, 'zone_code'),
            nature_of_property=read_str(row, 16, 'nature_of_property'),
            primary_purpose=row[17] or None,
            strata_lot_number=read_optional_int(row, 18, 'strata_lot_number'),
            comp_code=row[19] or None,
            sale_code=row[20] or None,
            interest_of_sale=read_optional_int(row, 21, 'interest_of_sale'),
            dealing_number=read_str(row, 22, 'dealing_number'),
        )

    def create_c(self: Self, pos: int, row: List[str], b_record: Any, variant: Optional[str]):
        return t.SalePropertyLegalDescription(
            position=pos,
            parent=b_record,
            district_code=read_int(row, 0, 'district_code'),
            property_id=read_optional_int(row, 1, 'property_id'),
            sale_counter=read_int(row, 2, 'sale_counter'),
            date_provided=read_datetime(row, 3, 'date_provided'),
            property_description=row[4] or None,
        )

    def create_d(self: Self, pos: int, row: List[str], c_record: Any, variant: Optional[str]):
        return t.SaleParticipant(
            position=pos,
            parent=c_record,
            district_code=read_int(row, 0, 'district_code'),
            property_id=read_optional_int(row, 1, 'property_id'),
            sale_counter=read_int(row, 2, 'sale_counter'),
            date_provided=read_datetime(row, 3, 'date_provided'),
            participant=read_str(row, 4, 'participant'),
        )

    def create_z(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SaleDataFileSummary(
            position=pos,
            parent=a_record,
            total_records=read_int(row, 0, 'total_records'),
            total_sale_property_details=read_int(row, 1, 'total_sale_property_details'),
            total_sale_property_legal_descriptions=read_int(row, 2, 'total_sale_property_legal_descriptions'),
            total_sale_participants=read_int(row, 3, 'total_sale_participants'),
        )

class Legacy2002Format(CurrentFormatFactory):
    zone_standard = 'legacy_vg_2011'
    zone_code_len = 4

    @classmethod
    def create(cls, year: int, file_path: str) -> 'Legacy2002Format':
        return Legacy2002Format(year, file_path)

    def create_a(self: Self, pos: int, row: List[str], variant: Optional[str]):
        return t.SaleRecordFile(
            position=pos,
            year_of_sale=self.year,
            file_path=self.file_path,
            file_type=None,
            district_code=read_int(row, 0, 'district_code'),
            date_provided=read_datetime(row, 1, 'date_provided'),
            submitting_user_id=row[2],
        )

    def create_c(self: Self, pos: int, row: List[str], b_record: Any, variant: Optional[str]):
        if variant is None:
            return super().create_c(pos, row, b_record, variant)
        elif variant == 'missing_property_id':
            return t.SalePropertyLegalDescription(
                position=pos,
                parent=b_record,
                district_code=read_int(row, 0, 'district_code'),
                property_id=None,
                sale_counter=read_int(row, 1, 'sale_counter'),
                date_provided=read_datetime(row, 2, 'date_provided'),
                property_description=row[3] or None,
            )
        else:
            raise TypeError(f'unknown variant {variant}')

    def create_d(self: Self, pos: int, row: List[str], c_record: Any, variant: Optional[str]):
        if variant is None:
            return super().create_d(pos, row, c_record, variant)
        elif variant == 'missing_property_id':
            return t.SaleParticipant(
                position=pos,
                parent=c_record,
                district_code=read_int(row, 0, 'district_code'),
                property_id=None,
                sale_counter=read_int(row, 1, 'sale_counter'),
                date_provided=read_datetime(row, 2, 'date_provided'),
                participant=read_str(row, 3, 'participant'),
            )
        else:
            raise TypeError(f'unknown variant {variant}')

class Legacy1990Format(AbstractFormatFactory):
    def __init__(self: Self, year: int, file_path: str):
        self.year = year
        self.file_path = file_path

    @classmethod
    def create(cls, year: int, file_path: str):
        return Legacy1990Format(year, file_path)

    def create_a(self: Self, pos: int, row: List[str], variant: Optional[str]):
        """
        Column 0 in the row will always be empty. I guess is this is
        to maintain some level of consistency with the later formats.
        """
        return t.SaleRecordFileLegacy(
            position=pos,
            file_path=self.file_path,
            year_of_sale=self.year,
            submitting_user_id=row[1],
            date_provided=read_datetime(row, 2, 'date_provided'),
        )

    def create_b(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SalePropertyDetails1990(
            position=pos,
            parent=a_record,
            district_code=read_int(row, 0, 'district_code'),
            source=row[1] or None,
            valuation_number=row[2] or None,
            property_id=read_optional_int(row, 3, 'property_id'),
            unit_number=row[4] or None,
            house_number=row[5] or None,
            street_name=row[6] or None,
            locality_name=row[7] or None,
            postcode=read_optional_int(row, 8, 'property_postcode'),
            contract_date=read_date_pre_2002(row, 9, 'contract_date'),
            purchase_price=read_float(row, 10, 'purchase_price'),
            land_description=row[11] or None,
            area=read_optional_float(row, 12, 'area'),
            area_type=read_area_type(row, 13, 'area_type'),
            dimensions=row[14] or None,
            comp_code=row[15] or None,
            zone_code=StrCheck(max_len=4).read_optional(row, 16, 'zone_code'),
            zone_standard=read_zone_std(row, 16, 'zone_standard'),
        )

    def create_c(self: Self, pos: int, row: List[str], b_record: Any, variant: Optional[str]):
        raise TypeError('c record not allowed in 1990 format')

    def create_d(self: Self, pos: int, row: List[str], c_record: Any, variant: Optional[str]):
        raise TypeError('d record not allowed in 1990 format')

    def create_z(self: Self, pos: int, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SaleDataFileSummary(
            position=pos,
            parent=a_record,
            total_records=read_int(row, 0, 'total_records'),
            total_sale_property_details=read_int(row, 1, 'total_sale_property_details'),

            # field not provided in 1990 format
            total_sale_property_legal_descriptions=0,
            total_sale_participants=0,
        )

def parse_datetime(date_str):
    return datetime.strptime(date_str, "%Y%m%d %H:%M")

def parse_date(date_str):
    return datetime.strptime(date_str, "%Y%m%d")

def parse_date_pre_2002(date_str):
    return datetime.strptime(date_str, "%d/%m/%Y")

T = TypeVar("T")

def mk_read_date(f: Callable[[str], datetime]) -> Callable[[List[str], int, str], datetime]:
    def impl(row: List[str], idx: int, name: str) -> datetime:
        try:
            return f(row[idx])
        except Exception as e:
            message = 'Failed to read INT %s @ row[%s] IN %s' % (name, idx, row)
            raise Exception(message) from e
    return impl

def mk_read_optional(f: Callable[[List[str], int, str], T]) -> Callable[[List[str], int, str], T | None]:
    def impl(row: List[str], idx: int, name: str) -> T | None:
        if row[idx] == '':
            return None
        else:
            return f(row, idx, name)
    return impl

@dataclass
class StrCheck:
    min_len: int | None = field(default=None)
    max_len: int | None = field(default=None)

    def read_optional(self, *args, **kwargs) -> str | None:
        return mk_read_optional(self.read)(*args, **kwargs)

    def read(self, row: List[str], idx: int, name: str) -> str:
        if not row[idx]:
            message = 'Failed to read STR %s @ row[%s] IN %s' % (name, idx, row)
            raise Exception(message)

        s = row[idx]

        if self.min_len and len(s) < self.min_len:
            message = '%s too long (%s @ row[%s] IN %s)' % (s, name, idx, row)
            raise Exception(message)

        if self.max_len and len(s) > self.max_len:
            message = '%s too long (%s @ row[%s] IN %s)' % (s, name, idx, row)
            raise Exception(message)

        return s

def read_zone_std(row: List[str], idx: int, name: str) -> t.ZoningKind | None:
    if not row[idx]:
        return None

    legacy_zone = [
        'A', 'B', 'C', 'D', 'E',
        'I', 'M', 'N', 'O', 'P',
        'R', 'S', 'T', 'U', 'V',
        'W', 'X', 'Y', 'Z',
    ]

    col = row[idx]
    if col in legacy_zone:
        return 'legacy_vg_2011'

    epaa_zone_prefixes = [
        'IN', 'MU', 'RE', 'RU', 'SP',
        'B', 'C', 'E', 'R', 'W',
    ]

    for p in epaa_zone_prefixes:
        if len(col) != len(p)+1:
            continue
        if not col.startswith(p):
            continue
        if not col[-1].isdigit():
            continue
        return 'ep&a_2006'

    return 'unknown'


def read_str(row: List[str], idx: int, name: str) -> str:
    if not row[idx]:
        message = 'Failed to read STR %s @ row[%s] IN %s' % (name, idx, row)
        raise Exception(message)
    return row[idx]


def read_int(row: List[str], idx: int, name: str) -> int:
    try:
        return int(row[idx])
    except Exception as e:
        message = 'Failed to read INT %s @ row[%s] IN %s' % (name, idx, row)
        raise Exception(message) from e

def read_float(row: List[str], idx: int, name: str) -> float:
    try:
        return float(row[idx])
    except Exception as e:
        message = 'Failed to read FLOAT %s @ row[%s] IN %s' % (name, idx, row)
        raise Exception(message) from e


def read_area_type(row: List[str], idx: int, name: str) -> str | None:
    match row[idx]:
        case 'M': return 'M'
        case 'H': return 'H'
        case 'U': return 'U'

        # Unknown area unit
        case '': return None
        case other: raise ValueError(f'Unknown area unit {other}')

read_optional_int = mk_read_optional(read_int)
read_optional_float = mk_read_optional(read_float)
read_date = mk_read_date(parse_date)
read_date_pre_2002 = mk_read_date(parse_date_pre_2002)
read_datetime = mk_read_date(parse_datetime)
read_optional_date = mk_read_optional(read_date)
read_optional_datetime = mk_read_optional(read_datetime)
