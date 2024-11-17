import asyncio
from dataclasses import dataclass
from datetime import date
from logging import getLogger
from typing import Dict, Optional, Self
import uuid

from lib.service.database import DatabaseService
from lib.service.io import IoService
from lib.pipeline.nsw_lrs.property_description.data import PropertyDescription
from lib.pipeline.nsw_lrs.property_description.parse import parse_property_description_data
from lib.tooling.schema import SchemaController, SchemaDiscovery, Command

@dataclass
class Quantile:
    start: Optional[int]
    end: Optional[int]

@dataclass
class NswLsrPropDescCommand:
    legal_descriptions: Quantile
    legal_description_by_strata_lot: Quantile

_logger = getLogger(__name__)

async def process_property_description(db: DatabaseService,
                                       io: IoService,
                                       workers: int) -> None:
    controller = SchemaController(io, db, SchemaDiscovery.create(io))
    semaphore = asyncio.Semaphore(1)
    ingestor = PropertyDescriptionIngestion(db, semaphore)
    await ingestor.run(workers)

class PropertyDescriptionIngestion:
    _logger = getLogger(f'{__name__}.PropertyDescriptionIngestion')
    _db: DatabaseService
    _semaphore: asyncio.Semaphore

    def __init__(self: Self, db, semaphore) -> None:
        self._db = db
        self._semaphore = semaphore

    async def run(self: Self, workers: int) -> None:
        async with self._db.async_connect() as c, c.cursor() as cursor:
            self._logger.info('preparing shards')
            a, b = await asyncio.gather(
                find_table_quantiles(cursor, 'nsw_lrs.legal_description', workers),
                find_table_quantiles(cursor, 'nsw_lrs.legal_description_by_strata_lot', workers),
            )

        shards = [NswLsrPropDescCommand(a_v, b[a_k]) for a_k, a_v in a.items()]
        tasks = [asyncio.create_task(self._run_worker(c, i)) for i, c in enumerate(shards)]
        self._logger.info("Running nsw_lrs.legal_description ingestion")
        await asyncio.gather(*tasks)
        self._logger.info("Complete")

    async def _run_worker(self: Self, task: NswLsrPropDescCommand, i: int):
        await self._chunk_quatile('nsw_lrs.legal_description', task.legal_descriptions, i)

    async def _chunk_quatile(self: Self, table_name: str, q: Quantile, i: int) -> None:
        limit = 100
        temp_table_name = f"q_{uuid.uuid4().hex[:8]}"

        async with self._db.async_connect() as conn, conn.cursor() as cursor:
            async with self._semaphore:
                await cursor.execute(f"""
                    SET session_replication_role = 'replica';

                    CREATE TEMP TABLE pg_temp.{temp_table_name} AS
                    SELECT source_id, legal_description, property_id, effective_date
                      FROM nsw_lrs.legal_description
                      LEFT JOIN meta.source_byte_position USING (source_id)
                      LEFT JOIN meta.file_source USING (file_source_id)
                     WHERE legal_description_kind = '> 2004-08-17'
                       {f"AND source_id >= {q.start}" if q.start else ''}
                       {f"AND source_id < {q.end}" if q.end else ''};
                """)

            await cursor.execute(f"SELECT count(*) FROM pg_temp.{temp_table_name}")
            count = (await cursor.fetchone())[0]

            for offset in range(0, count, limit):
                self._logger.info(f"[{i}] {temp_table_name}: {offset}/{count}")
                await cursor.execute(f"""
                    SELECT source_id, legal_description, property_id, effective_date
                      FROM pg_temp.{temp_table_name}
                      LIMIT {limit} OFFSET {offset}
                """)

                rows = [
                    (
                        source,
                        parse_property_description_data(description),
                        property,
                        effective_date,
                    )
                    for source, description, property, effective_date in await cursor.fetchall()
                ]

                try:
                    await cursor.executemany("""
                        INSERT INTO nsw_lrs.parcel (
                            parcel_id,
                            parcel_plan,
                            parcel_section,
                            parcel_lot)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (parcel_id) DO NOTHING;
                    """, [
                        (parcel.id, parcel.plan, parcel.section, parcel.lot)
                        for source, property_desc, property, effective_date in rows
                        for parcel in property_desc.parcels.all
                    ])
                    await conn.commit()

                    await cursor.executemany("""
                        INSERT INTO nsw_lrs.property_parcel_assoc(
                            source_id,
                            effective_date,
                            property_id,
                            parcel_id,
                            partial)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, [
                        (source, effective_date, property, parcel_id, partial)
                        for source, property_desc, property, effective_date in rows
                        for parcel_id, partial in [
                            *((p.id, True) for p in property_desc.parcels.partial),
                            *((p.id, False) for p in property_desc.parcels.complete),
                        ]
                    ])
                except Exception as e:
                    raise e

            self._logger.info(f"[{i}] {temp_table_name}: DONE")
            await conn.commit()
            await cursor.execute(f"""
                DROP TABLE pg_temp.{temp_table_name};
                SET session_replication_role = 'origin';
            """)


async def find_table_quantiles(cursor, table: str, quantiles: int) -> Dict[int, Quantile]:
    await cursor.execute(f"""
        SELECT segment, MIN(source_id), MAX(source_id)
        FROM (SELECT source_id, NTILE({quantiles}) OVER (ORDER BY source_id) AS segment
                FROM {table}
               WHERE legal_description_kind = '> 2004-08-17') t
        GROUP BY segment
        ORDER BY segment
    """)
    items = await cursor.fetchall()
    return {
        row[0]: Quantile(
            start=row[1] if i > 0 else None,
            end=row[2] if i < len(items)-1 else None,
        )
        for i, row in enumerate(items)
    }

