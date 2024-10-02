from dataclasses import dataclass
from typing import Self, Dict, Any

@dataclass
class DatabaseConfig:
    dbname: str
    host: str
    port: int
    user: str
    password: str

    @property
    def psycopg2_url(self: Self) -> str:
        return \
            f"postgresql+psycopg2://" \
            f"{self.user}:" \
            f"{self.password}@" \
            f"{self.host}:" \
            f"{self.port}/{self.dbname}"

    @property
    def kwargs(self: Self) -> Dict[str, Any]:
        return {
            'host': self.host,
            'user': self.user,
            'port': self.port,
            'password': self.password,
        }
