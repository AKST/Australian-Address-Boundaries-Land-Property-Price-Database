from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Generic, Self, List, TypeVar, Optional, Protocol

from .zoning import ZoningKind

def parse_datetime(date_str):
    return datetime.strptime(date_str, "%Y%m%d %H:%M")

def parse_date(date_str):
    return datetime.strptime(date_str, "%Y%m%d")

def parse_date_pre_2002(date_str):
    return datetime.strptime(date_str, "%d/%m/%Y")

T = TypeVar("T")
K = TypeVar('K', contravariant=True)
V = TypeVar('V', covariant=True)

class Row(Protocol[K, V]):
    def __getitem__(self, key: K) -> V:
        pass

def read_zone_std(row: Row[K, str], idx: K, name: str) -> ZoningKind | None:
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

def mk_read_date(f: Callable[[str], datetime]) -> Callable[[Row[K, str], K, str], datetime]:
    def impl(row: Row[K, str], idx: K, name: str) -> datetime:
        try:
            return f(row[idx])
        except Exception as e:
            message = 'Failed to read INT %s @ row[%s] IN %s' % (name, idx, row)
            raise Exception(message) from e
    return impl

def mk_read_optional(f: Callable[[Row[K, str], K, str], T]) -> Callable[[Row[K, str], K, str], T | None]:
    def impl(row: Row[K, str], idx: K, name: str) -> T | None:
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

    def read(self, row: Row[K, str], idx: K, name: str) -> str:
        if not row[idx]:
            name = name or idx # type: ignore
            message = 'Failed to read STR %s @ row[%s] IN %s' % (name, idx, row)
            raise Exception(message)

        s = row[idx]

        if self.min_len and len(s) < self.min_len:
            name = name or idx # type: ignore
            message = '%s too long (%s @ row[%s] IN %s)' % (s, name, idx, row)
            raise Exception(message)

        if self.max_len and len(s) > self.max_len:
            name = name or idx # type: ignore
            message = '%s too long (%s @ row[%s] IN %s)' % (s, name, idx, row)
            raise Exception(message)

        return s

def read_postcode(row: Row[K, str], idx: K, name: str) -> Optional[str]:
    match row[idx]:
        case None: return None
        case other if len(other) != 4: return None
        case other: return row[idx]

def read_str(row: Row[K, str], idx: K, name: str) -> str:
    if not row[idx]:
        message = 'Failed to read STR %s @ row[%s] IN %s' % (name, idx, row)
        name = name or idx # type: ignore
        raise Exception(message)
    return row[idx]

def read_int(row: Row[K, str], idx: K, name: str) -> int:
    try:
        return int(row[idx])
    except Exception as e:
        name = name or idx # type: ignore
        message = 'Failed to read INT %s @ row[%s] IN %s' % (name, idx, row)
        raise Exception(message) from e

def read_float(row: Row[K, str], idx: K, name: str) -> float:
    try:
        return float(row[idx])
    except Exception as e:
        name = name or idx # type: ignore
        message = 'Failed to read FLOAT %s @ row[%s] IN %s' % (name, idx, row)
        raise Exception(message) from e


def read_area_type(row: Row[K, str], idx: K, name: str) -> str | None:
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
read_optional_date_pre_2002 = mk_read_optional(read_date_pre_2002)

