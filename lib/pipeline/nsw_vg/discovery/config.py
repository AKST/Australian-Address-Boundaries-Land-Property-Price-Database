from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from lib.service.static_environment import Target

@dataclass
class NswVgTarget(Target):
    datetime: datetime

@dataclass(frozen=True)
class NswVgDiscoveryConfig:
    id: str
    """
    The NSW Valuer Generals bulk data pages have identical
    layouts, so we can reuse parser this "scrape attempt"
    code between them. There's really only 2 pages, one
    for property sales and one for land values.
    """
    prefix: str
    directory_page: str
    css_class_path: List[Tuple[str, str]]
    cache_period: str
    date_fmt: str

