from datetime import datetime
from typing import Dict, Optional, Tuple, Type

from lib.nsw_vg.property_sales.file_format import factories

FactoryMap = Type[factories.AbstractFormatFactory]
Syntax = Dict[str, int]

SYNTAX_1990: Syntax = { 'A': 5, 'B': 21, 'Z': 3 }
SYNTAX_2002: Syntax = { 'A': 4, 'B': 24, 'C': 6, 'D': 11, 'Z': 5 }
SYNTAX_2012: Syntax = { 'A': 5, 'B': 24, 'C': 6, 'D': 11, 'Z': 5 }
SYNTAX_2021: Syntax = { 'A': 5, 'B': 24, 'C': 6, 'D': 12, 'Z': 5 }

def get_columns_and_syntax(download_date: Optional[datetime],
                           published_year: int) -> Tuple[FactoryMap, Syntax] | Tuple[None, None]:
    """
    Yes syntax and factories can be fairly arbitrary
    specified relative to the download date and year
    published.

    This function largely narrows down the variation.
    """
    if download_date is None and published_year <= 2001:
        return factories.Legacy1990Format, SYNTAX_1990
    elif download_date is None:
        raise TypeError('missing datetime, cannot parse')

    match (download_date, published_year):
        case (d, 2001) if d.year > 2001:
            return factories.Legacy2002Format, SYNTAX_2002
        case (d, 2001) if d.month < 6:
            return factories.Legacy1990Format, SYNTAX_1990
        case (d, 2001) if d.month < 9:
            return None, None
            # return factories.Legacy2002Format, SYNTAX_2002
        case (d, 2001):
            return factories.Legacy2002Format, SYNTAX_2002
        case (_, y) if y < 2012:
            return factories.Legacy2002Format, SYNTAX_2002
        case (d, 2012) if d.month < 3:
            return factories.Legacy2002Format, SYNTAX_2002
        case (d, 2012) if d.month == 3 and d.day < 13:
            return factories.Legacy2002Format, SYNTAX_2002
        case (d, 2012):
            return factories.CurrentFormatFactory, SYNTAX_2012
        case (d, 2021) if d.month == 8 and d.day == 23:
            return factories.CurrentFormatFactory, SYNTAX_2021
        case (d, 2021):
            return factories.CurrentFormatFactory, SYNTAX_2012
        case _:
            return factories.CurrentFormatFactory, SYNTAX_2012



