import logging
from typing import Optional

def config_logging(worker: Optional[int], debug: bool = False):
    format_str = '[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s'
    format_str = f'[{worker}]{format_str}' if worker is not None else format_str
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(level=level, format=format_str, datefmt='%Y-%m-%d %H:%M:%S')

def config_vendor_logging():
    logging.getLogger('sqlglot').setLevel(logging.ERROR)
    logging.getLogger('psycopg.pool').setLevel(logging.ERROR)
