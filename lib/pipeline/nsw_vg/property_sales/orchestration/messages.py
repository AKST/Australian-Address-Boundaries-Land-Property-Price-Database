from dataclasses import dataclass

from ..data import PropertySaleDatFileMetaData

class ParentMessage:
    class Message:
        pass

    @dataclass
    class ImDead(Message):
        sender: int

class ChildMessage:
    class Message:
        pass

    @dataclass
    class Parse(Message):
        file: PropertySaleDatFileMetaData

    @dataclass
    class RequestClose(Message):
        pass
