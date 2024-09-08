from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Target:
    url: str
    web_dst: str
    zip_dst: Optional[str] = field(default=None)
    token: Optional[str] = field(default=None)