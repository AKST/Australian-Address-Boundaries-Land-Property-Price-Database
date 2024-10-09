from dataclasses import dataclass
import logging
from typing import Callable, Dict, List, Self, Tuple, Iterator, Literal, Optional

from lib.service.database import DatabaseConfig
from lib.service.static_environment.config import Target

@dataclass
class IngestionConfig:
    worker_count: int
    worker_config: 'WorkerConfig'
    ingest_sources: List['IngestionSource']

@dataclass
class WorkerArgs:
    child: int
    tasks: List[Tuple[str, 'IngestionSource']]
    source_root_dir: str
    worker_config: 'WorkerConfig'

@dataclass
class WorkerConfig:
    db_config: DatabaseConfig
    db_connections: int
    log_config: Optional['WorkerLogConfig']

@dataclass
class WorkerLogConfig:
    level: int
    format: str
    datefmt: Optional[str]


@dataclass
class IngestionSource:
    gpkg_file: str
    static_file_target: Target

    """
    This should be the maping of the layer from
    the dataframe file to database table name.
    """
    layer_to_table: Dict[str, str]

    """
    These should be the database column names derived
    from dataframe column names.
    """
    database_column_names_for_dataframe_columns: Dict[str, Dict[str, str]]

    @property
    def gpkg_export_path(self: Self) -> str:
        return f'{self.static_file_target.zip_dst}/{self.gpkg_file}'

    def tables(self: Self) -> Iterator[str]:
        yield from self.layer_to_table.values()


