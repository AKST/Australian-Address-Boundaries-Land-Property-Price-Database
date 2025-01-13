from dataclasses import dataclass
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
        pass

    @dataclass
    class Ingestion:
        staging: Optional['GisTaskConfig.StageApiData']
        deduplication: Optional['GisTaskConfig.Deduplication']

