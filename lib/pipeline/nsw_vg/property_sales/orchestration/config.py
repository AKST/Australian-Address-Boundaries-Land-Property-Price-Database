from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional, Self

from lib.service.database import DatabaseConfig
from ..data import PropertySaleDatFileMetaData
from ..ingestion.config import IngestionConfig


@dataclass
class ParentConfig:
    target_root_dir: str
    publish_min: Optional[int]
    publish_max: Optional[int]
    download_min: Optional[date]
    download_max: Optional[date]

    def valid_download_date(self: Self, maybe_dt: Optional[datetime]) -> bool:
        if maybe_dt is None:
            return self.download_min is None and self.download_max is None

        match (maybe_dt.date(), self.download_min, self.download_max):
            case (d, mn, _) if mn is not None and d < mn: return False
            case (d, _, mx) if mx is not None and d > mx: return False
            case _: return True

    def valid_publish_date(self: Self, maybe_year: Optional[int]) -> bool:
        if maybe_year is None:
            return self.publish_min is None and self.publish_max is None

        match (maybe_year, self.publish_min, self.publish_max):
            case (y, mn, _) if mn is not None and y < mn: return False
            case (y, _, mx) if mx is not None and y > mx: return False
            case _: return True

@dataclass
class ChildConfig:
    file_limit: int | None
    parser_chunk_size: int
    db_pool_size: int
    db_batch_size: int
    db_config: DatabaseConfig
    ingestion_config: IngestionConfig
    debug_logs: bool = field(default=False)

class ParentMessage:
    class Message:
        pass

    @dataclass
    class IDied(Message):
        sender: int

class ChildMessage:
    class Message:
        pass

    @dataclass
    class Parse(Message):
        file: PropertySaleDatFileMetaData

    @dataclass
    class Done(Message):
        pass
