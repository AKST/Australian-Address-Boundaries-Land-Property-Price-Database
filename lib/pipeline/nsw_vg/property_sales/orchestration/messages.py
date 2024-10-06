from dataclasses import dataclass

from .telemetry import IngestionSample
from ..data import PropertySaleDatFileMetaData


class ParentMessage:
    @dataclass
    class Message:
        sender: int

    @dataclass
    class Update(Message):
        value: IngestionSample

class ChildMessage:
    class Message:
        pass

    @dataclass
    class Parse(Message):
        file: PropertySaleDatFileMetaData

    @dataclass
    class RequestClose(Message):
        pass
