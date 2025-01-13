import logging
from typing import Literal, Optional, Set

def config_logging(worker: Optional[int], debug: bool = False):
    format_str = '[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s'
    format_str = f'[{worker}]{format_str}' if worker is not None else format_str
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=format_str, datefmt='%Y-%m-%d %H:%M:%S')

_Vendor = Literal['sqlglot', 'psycopg.pool', 'asyncio']

def config_vendor_logging(
        set_to_error: Set[_Vendor],
        disable: Optional[Set[_Vendor]] = None):
    disable = disable or set()

    for vendor in set_to_error:
        logging.getLogger(vendor).setLevel(logging.ERROR)

    for vendor in disable:
        logger = logging.getLogger(vendor)
        logger.disabled = True
