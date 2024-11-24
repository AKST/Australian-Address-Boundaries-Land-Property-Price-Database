from typing import Generator, List, Optional, Self, Set, Tuple
import re

from .types import LandParcel
from .. import data

VALID_PARCEL_ID_CHARS = re.compile(r'^[a-zA-Z0-9/]+$')
VALID_LOT_ID_CHARS = re.compile(r'^[a-zA-Z0-9]+$')

VALID_PARCEL_ID = re.compile(r'^\w+/(?:\w+/|/)?(SP)?\d+$')

VALID_PLAN_ID = re.compile(r'^(SP)?\d+$')
VALID_SECTION_ID = re.compile(r'^\w+$')
VALID_LOT_ID = re.compile(r'^\w+$')

def _valid_parcel(chunk: str) -> bool:
    if not bool(VALID_PARCEL_ID.match(chunk)):
        return False
    match chunk.count('/'):
        case 0:
            return False
        case 1:
            lot, plan = chunk.split('/')
            return _valid_parcel_lot(lot)
        case 2:
            lot, sec, plan = chunk.split('/')
            return _valid_parcel_lot(lot)
        case other:
            return False

def _valid_parcel_partial(chunk: str) -> bool:
    return bool(VALID_PARCEL_ID_CHARS.match(chunk)) \
       and 1 <= chunk.count('/') <= 2

def _valid_parcel_lot(chunk: str) -> bool:
    return bool(VALID_LOT_ID_CHARS.match(chunk)) and len(chunk) <= 5

def _valid_parcel_trailing_lot(chunk: str) -> bool:
    return chunk.endswith(',') and _valid_parcel_lot(chunk[:-1])

def parse_parcel_data(parcel_id: str) -> data.LandParcel:
    match parcel_id.count('/'):
        case 1:
            clean_id = parcel_id
            parcel_id = parcel_id.replace('/', '//')
        case 2:
            clean_id = parcel_id.replace('//', '/')
        case other:
            raise ValueError('too many slashes, {parcel_id}')

    match parcel_id.split('/'):
        case [lot, sec, plan] if not plan or len(plan) > 9 or not VALID_PLAN_ID.match(plan):
            raise ValueError(f'invalid plan, {parcel_id}')

        case [lot, sec, plan] if not lot or len(lot) > 5 or not VALID_LOT_ID.match(lot):
            raise ValueError(f'invalid section, {parcel_id}')

        case [lot, sec, plan] if sec and (len(sec) > 4 or not VALID_SECTION_ID.match(sec)):
            raise ValueError(f'invalid section, {parcel_id}')

        case [lot, '', plan]:
            return data.LandParcel(clean_id, lot, None, plan)

        case [lot, sec, plan]:
            return data.LandParcel(clean_id, lot, sec, plan)

    raise ValueError(f'invalid parcel, {parcel_id}')

class ParcelsParser:
    _stop = False
    _desc: str
    _seen: Set[LandParcel]
    _read_from = 0

    def __init__(self: Self, desc: str) -> None:
        self._desc = desc
        self._seen = set()

    @property
    def running(self: Self) -> bool:
        return not self._stop and self._read_from < len(self._desc)

    @property
    def remains(self: Self) -> str:
        return self._desc[self._read_from:]

    def read_parcels(self: Self) -> Generator[LandParcel, None, None]:
        chunk = None

        while self.running:
            chunk = self._read_chunk(skip=0)

            if _valid_parcel(chunk):
                yield LandParcel(id=chunk, part=False)
                self._move_cursor(1)
                continue

            next_chunk = self._read_chunk(skip=1)
            if 'PT' == chunk and _valid_parcel(next_chunk):
                yield LandParcel(id=next_chunk, part=True)
                self._move_cursor(2)
                continue

            if 'PT' != chunk and not chunk.endswith(','):
                return

            match self._read_compressed():
                case (plan, lots):
                    yield from (
                        LandParcel(id=f'{lot}{plan}', part=part)
                        for part, lot in lots
                    )
                case None:
                    return

    def _read_compressed(self: Self):
        lots: List[Tuple[bool, str]] = []
        while True:
            chunk = self._read_chunk()
            part = False

            if chunk == 'PT':
                part = True
                self._move_cursor(1)
                chunk = self._read_chunk()

            if _valid_parcel_trailing_lot(chunk):
                lots.append((part, chunk[:-1]))
                self._move_cursor(1)
            elif _valid_parcel_partial(chunk) and lots:
                if chunk[0] != '/':
                    lots.append((part, chunk[:chunk.find('/')]))
                plan = chunk[chunk.find('/'):]
                self._move_cursor(1)
                return plan, lots
            else:
                return None


    def _read_chunk(self: Self, skip = 0) -> str:
        copy = self._desc[self._read_from:]
        while skip > 0:
            copy = copy[copy.find(' ') + 1:]
            skip -= 1
        if copy.find(' ') == -1:
            return copy
        else:
            return copy[:copy.find(' ')]

    # todo rename to progress
    def _move_cursor(self: Self, skip = 0):
        while skip > 0:
            if self._desc[self._read_from:].find(' ') == -1:
                self._read_from = len(self._desc)
                return

            self._read_from += self._desc[self._read_from:].find(' ') + 1
            skip -= 1
