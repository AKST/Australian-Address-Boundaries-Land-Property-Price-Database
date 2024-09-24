from dataclasses import dataclass, field

@dataclass
class Target:
    url: str
    web_dst: str
    zip_dst: str | None = field(default=None)
    token: str | None = field(default=None)
