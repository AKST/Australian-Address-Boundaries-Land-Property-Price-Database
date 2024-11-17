from typing import Generator, List, Optional, Self, Set, Tuple
import re

from .types import LandParcel

VALID_PARCEL_ID_CHARS = re.compile(r'^[a-zA-Z0-9/]+$')
VALID_LOT_ID_CHARS = re.compile(r'^[a-zA-Z0-9]+$')

def _valid_parcel(chunk: str) -> bool:
    return bool(VALID_PARCEL_ID_CHARS.match(chunk)) \
       and '/' in chunk[1:] \
       and 1 <= chunk.count('/') <= 2

def _valid_parcel_partial(chunk: str) -> bool:
    return bool(VALID_PARCEL_ID_CHARS.match(chunk)) \
       and 1 <= chunk.count('/') <= 2

def _valid_parcel_lot(chunk: str) -> bool:
    return bool(VALID_LOT_ID_CHARS.match(chunk[:-1])) and chunk.endswith(',')

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

            if _valid_parcel_lot(chunk):
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
