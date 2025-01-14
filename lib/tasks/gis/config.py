from dataclasses import dataclass, field
from typing import List, Optional
from lib.pipeline.gis import DateRangeParam

class GisTaskConfig:
    @dataclass
    class StageApiData:
        db_workers: int
        skip_save: bool
        dry_run: bool
        gis_params: List[DateRangeParam]
        exp_backoff_attempts: int

    @dataclass
    class Deduplication:
        run_from: Optional[int]
        run_till: Optional[int]
        truncate: bool = field(default=False)
        drop_raw: bool = field(default=False)
        drop_dst_schema: bool = field(default=False)

    @dataclass
    class Ingestion:
        staging: Optional['GisTaskConfig.StageApiData']
        deduplication: Optional['GisTaskConfig.Deduplication']

