import asyncio
import psycopg
import time
from typing import Self
from sqlalchemy import create_engine

from .config import DatabaseConfig

class DatabaseService:
    config: DatabaseConfig

    def __init__(self: Self, config: DatabaseConfig) -> None:
        self.config = config

    def engine(self: Self):
        return create_engine(self.config.psycopg2_url)

    def connect(self: Self) -> psycopg.Connection:
        return psycopg.connect(
            dbname=self.config.dbname,
            **self.config.kwargs,
        )

    async def async_connect(self: Self) -> psycopg.AsyncConnection:
        return await psycopg.AsyncConnection.connect(
            dbname=self.config.dbname,
            **self.config.kwargs,
        )

    async def wait_till_running(self: Self, interval=5, timeout=60):
        start_time = time.time()
        while True:
            try:
                conn = await psycopg.AsyncConnection.connect(
                    dbname='postgres',
                    **self.config.kwargs,
                )
                await conn.close()
                break
            except psycopg.OperationalError as e:
                if time.time() - start_time > timeout:
                    raise e
                await asyncio.sleep(interval)

