from dataclasses import dataclass

from lib.service.database import DatabaseConfig
from lib.pipeline.nsw_vg.property_sales.data import PropertySaleDatFileMetaData

@dataclass
class NswVgPsChildConfig:
    file_limit: int

    db_pool_size: int
    db_batch_size: int
    db_config: DatabaseConfig

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

    @dataclass
    class Kill(Message):
        pass
