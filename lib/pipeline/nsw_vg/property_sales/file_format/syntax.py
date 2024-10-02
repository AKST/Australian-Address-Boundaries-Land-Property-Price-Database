from datetime import datetime
from typing import Dict, Optional, Tuple, Type, List

from .factories import AbstractFormatFactory, Legacy1990Format, Legacy2002Format, CurrentFormatFactory

MakeFactory = Type[AbstractFormatFactory]
Syntax = Dict[str, int | List[Tuple[int, Optional[str]]]]

# initial schema
SYNTAX_1990: Syntax = { 'A': 5, 'B': 21, 'Z': 3 }

# Used in data downloaded in July
SYNTAX_2001_07: Syntax = {
    'A': 4,
    'B': 24,
    'C': [(6, None), (5, 'missing_property_id')],
    'D': [(11, None), (10, 'missing_property_id')],
    'Z': 5,
}

# introduced after July 2001
SYNTAX_2002: Syntax = { 'A': 4, 'B': 24, 'C': 6, 'D': 11, 'Z': 5 }

SYNTAX_2012: Syntax = { 'A': 5, 'B': 24, 'C': 6, 'D': 11, 'Z': 5 }

# there's one day in 2021 where they did something different
# then pretended it never happened ever since.
SYNTAX_2021: Syntax = { 'A': 5, 'B': 24, 'C': 6, 'D': 12, 'Z': 5 }

def get_columns_and_syntax(download_date: Optional[datetime], published_year: int) -> Tuple[MakeFactory, Syntax]:
    """
    Yes syntax and factories can be fairly arbitrary
    specified relative to the download date and year
    published.

    This function largely narrows down the variation.
    """
    if download_date is None and published_year <= 2001:
        return Legacy1990Format, SYNTAX_1990
    elif download_date is None:
        raise TypeError('missing datetime, cannot parse')

    match (download_date, published_year):
        case (d, 2001) if d.year > 2001:
            return Legacy2002Format, SYNTAX_2002
        case (d, 2001) if d.month < 8:
            return Legacy2002Format, SYNTAX_2001_07
        case (d, 2001):
            return Legacy2002Format, SYNTAX_2002
        case (_, y) if y < 2012:
            return Legacy2002Format, SYNTAX_2002
        case (d, 2012) if d.month < 3:
            return Legacy2002Format, SYNTAX_2002
        case (d, 2012) if d.month == 3 and d.day < 13:
            return Legacy2002Format, SYNTAX_2002
        case (d, 2012):
            return CurrentFormatFactory, SYNTAX_2012
        case (d, 2021) if d.month == 8 and d.day == 23:
            return CurrentFormatFactory, SYNTAX_2021
        case (d, 2021):
            return CurrentFormatFactory, SYNTAX_2012
        case _:
            return CurrentFormatFactory, SYNTAX_2012

