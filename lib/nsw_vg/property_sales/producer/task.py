from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from lib.nsw_vg.discovery import NswVgTarget

@dataclass
class PropertySaleIngestionTask:
    target: NswVgTarget
    dat_path: str
    download_date: Optional[datetime]

    @property
    def target_year(self) -> int:
        if self.target.datetime is None:
            raise TypeError(f'unset datetime for target {self.target}')
        return self.target.datetime.year
