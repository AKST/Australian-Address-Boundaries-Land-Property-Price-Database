import abc
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Self, List, TypeVar, Optional

from lib.nsw_vg.property_sales import data as t

class AbstractFormatFactory(abc.ABC):
    @classmethod
    def create(cls, year: int, file_path: str) -> 'AbstractFormatFactory':
        raise NotImplementedError('create not implemented on AbstractFormatFactory')

    @abc.abstractmethod
    def create_a(self: Self, row: List[str], variant: Optional[str]) -> t.BasePropertySaleFileRow:
        pass

    @abc.abstractmethod
    def create_b(self: Self, row: List[str], a_record: Any, variant: Optional[str]) -> t.BasePropertySaleFileRow:
        pass

    @abc.abstractmethod
    def create_c(self: Self, row: List[str], b_record: Any, variant: Optional[str]) -> t.SalePropertyLegalDescription:
        pass

    @abc.abstractmethod
    def create_d(self: Self, row: List[str], c_record: Any, variant: Optional[str]) -> t.SaleParticipant:
        pass

    @abc.abstractmethod
    def create_z(self: Self, row: List[str], a_record: Any, variant: Optional[str]) -> t.SaleDataFileSummary:
        pass

class CurrentFormatFactory(AbstractFormatFactory):
    def __init__(self: Self, year: int, file_path: str):
        self.year = year
        self.file_path = file_path
        self.area_parser = AreaParser(area_column=10, area_type_column=11)

    @classmethod
    def create(Cls, year: int, file_path: str) -> 'CurrentFormatFactory':
        return CurrentFormatFactory(year, file_path)

    def create_a(self: Self, row: List[str], variant: Optional[str]):
        return t.SaleRecordFile(
            year=self.year,
            file_path=self.file_path,
            file_type=row[0],
            district=read_int(row, 1, 'district'),
            date_downloaded=read_datetime(row, 2, 'date_downloaded'),
            submitting_user_id=row[3],
        )

    def create_b(self: Self, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SalePropertyDetails(
            parent=a_record,
            common=t.SalePropertyDetailsCommon(
                district=read_int(row, 0, 'district'),
                property_id=row[1],
                address=t.Address(
                    property_name=row[4] or None,
                    unit_number=row[5] or None,
                    house_number=row[6] or None,
                    street_name=row[7] or None,
                    locality=row[8] or None,
                    postcode=read_optional_int(row, 9, 'property_postcode'),
                ),

                # columns 10 & 11
                area=self.area_parser.from_row(row),
                contract_date=read_optional_date(row, 12, 'contract_date'),
                purchase_price=read_optional_float(row, 14, 'purchase_price'),
                zoning=self._read_b_zoning(row),
                comp_code=row[19] or None,
            ),

            sale_counter=read_int(row, 2, 'sale_counter'),
            date_downloaded=read_datetime(row, 3, 'date_downloaded'),
            settlement_date=read_optional_date(row, 13, 'settlement_date'),
            nature_of_property=row[16] or None,
            primary_purpose=row[17] or None,
            strata_lot_number=row[18] or None,
            sale_code=row[20] or None,
            interest_of_sale=read_optional_int(row, 21, 'interest_of_sale'),
            dealing_number=row[22],
        )

    def create_c(self: Self, row: List[str], b_record: Any, variant: Optional[str]):
        return t.SalePropertyLegalDescription(
            parent=b_record,
            district=read_int(row, 0, 'district'),
            property_id=read_optional_int(row, 1, 'property_id'),
            sale_counter=row[2],
            date_downloaded=read_datetime(row, 3, 'date_downloaded'),
            property_legal_description=row[4],
        )

    def create_d(self: Self, row: List[str], c_record: Any, variant: Optional[str]):
        return t.SaleParticipant(
            parent=c_record,
            district=read_int(row, 0, 'district'),
            property_id=read_optional_int(row, 1, 'property_id'),
            sale_counter=row[2],
            date_downloaded=read_datetime(row, 3, 'date_downloaded'),
            participant=row[4],
        )

    def create_z(self: Self, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SaleDataFileSummary(
            parent=a_record,
            total_records=read_int(row, 0, 'total_records'),
            total_sale_property_details=read_int(row, 1, 'total_sale_property_details'),
            total_sale_property_legal_descriptions=read_int(row, 2, 'total_sale_property_legal_descriptions'),
            total_sale_participants=read_int(row, 3, 'total_sale_participants'),
        )

    def _read_b_zoning(self, row) -> Optional[t.AbstractZoning]:
        return read_zoning(row, 15, 'zoning')

class Legacy2002Format(CurrentFormatFactory):
    @classmethod
    def create(cls, year: int, file_path: str) -> 'Legacy2002Format':
        return Legacy2002Format(year, file_path)

    def create_a(self: Self, row: List[str], variant: Optional[str]):
        return t.SaleRecordFile(
            year=self.year,
            file_path=self.file_path,
            file_type=None,
            district=read_int(row, 0, 'district'),
            date_downloaded=read_datetime(row, 1, 'date_downloaded'),
            submitting_user_id=row[2],
        )

    def create_c(self: Self, row: List[str], b_record: Any, variant: Optional[str]):
        if variant is None:
            return super().create_c(row, b_record, variant)
        elif variant == 'missing_property_id':
            return t.SalePropertyLegalDescription(
                parent=b_record,
                district=read_int(row, 0, 'district'),
                property_id=None,
                sale_counter=row[1],
                date_downloaded=read_datetime(row, 2, 'date_downloaded'),
                property_legal_description=row[3],
            )
        else:
            raise TypeError(f'unknown variant {variant}')

    def create_d(self: Self, row: List[str], c_record: Any, variant: Optional[str]):
        if variant is None:
            return super().create_d(row, c_record, variant)
        elif variant == 'missing_property_id':
            return t.SaleParticipant(
                parent=c_record,
                district=read_int(row, 0, 'district'),
                property_id=None,
                sale_counter=row[1],
                date_downloaded=read_datetime(row, 2, 'date_downloaded'),
                participant=row[3],
            )
        else:
            raise TypeError(f'unknown variant {variant}')

    def _read_b_zoning(self, row) -> Optional[t.AbstractZoning]:
        return read_legacy_zoning(row, 15, 'zoning')

class Legacy1990Format(AbstractFormatFactory):
    def __init__(self: Self, year: int, file_path: str):
        self.year = year
        self.file_path = file_path
        self.area_parser = AreaParser(area_column=12, area_type_column=13)


    @classmethod
    def create(cls, year: int, file_path: str):
        return Legacy1990Format(year, file_path)

    def create_a(self: Self, row: List[str], variant: Optional[str]):
        return t.SaleRecordFile(
            year=self.year,
            file_path=self.file_path,
            file_type=None,
            district=read_optional_int(row, 0, 'district'),
            submitting_user_id=row[1],
            date_downloaded=read_datetime(row, 2, 'date_downloaded'),
        )

    def create_b(self: Self, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SalePropertyDetails1990(
            parent=a_record,
            common=t.SalePropertyDetailsCommon(
                district=read_int(row, 0, 'district'),
                property_id=row[3],
                address=t.Address(
                    property_name=None,
                    unit_number=row[4] or None,
                    house_number=row[5] or None,
                    street_name=row[6] or None,
                    locality=row[7] or None,
                    postcode=read_optional_int(row, 8, 'property_postcode'),
                ),
                contract_date=read_date_pre_2002(row, 9, 'contract_date'),
                purchase_price=read_float(row, 10, 'purchase_price'),
                # columns 12 & 13
                area=self.area_parser.from_row(row),

                comp_code=row[15] or None,
                zoning=read_legacy_zoning(row, 16, 'zoning'),
            ),
            source=row[1],
            valuation_num=row[2],
            land_description=row[11],
            dimensions=read_optional_str(row, 14, 'dimensions'),
        )

    def create_c(self: Self, row: List[str], b_record: Any, variant: Optional[str]):
        raise TypeError('c record not allowed in 1990 format')

    def create_d(self: Self, row: List[str], c_record: Any, variant: Optional[str]):
        raise TypeError('d record not allowed in 1990 format')

    def create_z(self: Self, row: List[str], a_record: Any, variant: Optional[str]):
        return t.SaleDataFileSummary(
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

@dataclass
class AreaParser:
    area_column: int
    area_type_column: int

    def from_row(self: Self, row: List[str]) -> Optional[t.Area]:
        area = read_optional_float(row, self.area_column, 'area')

        if area is None:
            return None

        match read_optional_str(row, self.area_type_column, 'area_type'):
            case 'M': return t.Area(amount=area, unit='M')
            case 'H': return t.Area(amount=area, unit='H')
            case 'U': return t.Area(amount=area, unit='U', stardard=False)

            # Unknown area unit
            case None: return t.Area(amount=area, unit=None, stardard=False)
            case other: raise ValueError(f'Unknown area unit {other}')

read_legacy_zoning = mk_read_optional(lambda row, idx, _: t.ZoningLegacy(zone=row[idx]))
read_zoning = mk_read_optional(lambda row, idx, _: t.ZoningPost2011(zone=row[idx]))

read_optional_str = mk_read_optional(lambda row, idx, _: row[idx])
read_optional_int = mk_read_optional(read_int)
read_optional_float = mk_read_optional(read_float)
read_date = mk_read_date(parse_date)
read_date_pre_2002 = mk_read_date(parse_date_pre_2002)
read_datetime = mk_read_date(parse_datetime)
read_optional_date = mk_read_optional(read_date)
read_optional_datetime = mk_read_optional(read_datetime)
