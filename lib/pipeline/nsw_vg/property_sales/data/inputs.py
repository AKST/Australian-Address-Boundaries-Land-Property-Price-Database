from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class PropertySaleDatFileMetaData:
    file_path: str
    published_year: int
    download_date: Optional[datetime]
    size: int
