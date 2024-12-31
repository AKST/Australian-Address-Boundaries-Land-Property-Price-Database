from dataclasses import dataclass

from ..discovery import NswVgTarget

class NswVgLvTaskDesc:
    @dataclass
    class Parse:
        file: str
        target: NswVgTarget
