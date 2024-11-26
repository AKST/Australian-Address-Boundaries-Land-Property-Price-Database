from dataclasses import dataclass, field
from typing import Optional
from lib.pipeline.nsw_vg.config import *
from lib.service.database import DatabaseConfig

class NswVgTaskConfig:
    @dataclass
    class PropDescIngest:
        worker_debug: bool
        workers: int
        sub_workers: int
        db_config: DatabaseConfig

    @dataclass
    class PsiIngest:
        worker_count: int
        worker_config: NswVgPsiWorkerConfig
        parent_config: NswVgPsiSupervisorConfig

    @dataclass
    class Dedup:
        run_from: int
        run_till: int
        truncate: bool = field(default=False)
        drop_raw: bool = field(default=False)
        drop_dst_schema: bool = field(default=False)

    @dataclass
    class LvIngest:
        truncate_raw_earlier: bool = field(default=False)

    @dataclass
    class Ingestion:
        load_raw_land_values: Optional['NswVgTaskConfig.LvIngest']
        load_raw_property_sales: Optional['NswVgTaskConfig.PsiIngest']
        deduplicate: Optional['NswVgTaskConfig.Dedup']
        property_descriptions: Optional['NswVgTaskConfig.PropDescIngest']

