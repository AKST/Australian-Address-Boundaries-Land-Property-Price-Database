from datetime import datetime
import pytest

from ..syntax import get_columns_and_syntax
from ..syntax import SYNTAX_1990, SYNTAX_2001_07, SYNTAX_2002, SYNTAX_2021, SYNTAX_2012

def test_special_2021_syntax():
    """
    On this date they did something different and had a
    slight variation in the format which they never really
    did since, and immediately went back to the 2012 format.
    """
    date = datetime(2021, 8, 23)
    col, syn = get_columns_and_syntax(date, 2021)
    assert syn == SYNTAX_2021

@pytest.mark.parametrize("year, date", [
    *[(y, datetime(2024, 1, 1)) for y in range(2013, 2025)],
    (2012, datetime(2012, 3, 13)),
    (2012, datetime(2012, 4, 1)),
    (2012, datetime(2021, 8, 22)),
    (2012, datetime(2021, 8, 24)),
])
def test_2012_syntax(year: int, date: datetime):
    """
    As of writting this (2024/09/26) this is
    the current default syntax, note 2021 is
    only to cover a single day.
    """
    col, syn = get_columns_and_syntax(date, year)
    assert syn == SYNTAX_2012

@pytest.mark.parametrize("year, date", [
    *[(y, datetime(y, 1, 1)) for y in range(2002, 2012)],
    *[(2001, datetime(y, 1, 1)) for y in range(2002, 2012)],
    (2012, datetime(2012, 3, 1)),
    (2012, datetime(2012, 3, 12)),
    (2012, datetime(2012, 1, 25)),
    (2012, datetime(2012, 2, 25)),
])
def test_post_2002_syntax(year: int, date: datetime):
    col, syn = get_columns_and_syntax(date, year)
    assert syn == SYNTAX_2002

@pytest.mark.parametrize("year, date", [
    *[(y, None) for y in range(1980, 2001)],
])
def test_post_1990_syntax(year: int, date: datetime | None):
    col, syn = get_columns_and_syntax(date, year)
    assert syn == SYNTAX_1990

@pytest.mark.parametrize("year, date", [
    *[(2001, datetime(2001, m, 1)) for m in range(1, 7)],
])
def test_2002_july_syntax(year: int, date: datetime | None):
    col, syn = get_columns_and_syntax(date, year)
    assert syn == SYNTAX_2001_07






