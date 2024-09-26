from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from lib.nsw_vg.discovery import NswVgTarget

@dataclass
class PropertySaleDatFileMetaData:
    file_path: str
    published_year: int
    download_date: Optional[datetime]
    size: int
