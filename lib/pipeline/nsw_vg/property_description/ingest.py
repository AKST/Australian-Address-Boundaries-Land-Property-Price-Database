import asyncio
from dataclasses import dataclass
from datetime import date
from logging import getLogger
from typing import Dict
import uuid

from lib.service.database import DatabaseService
from lib.service.io import IoService
from lib.pipeline.nsw_lrs.property_description.data import PropertyDescription
from lib.pipeline.nsw_lrs.property_description.parse import parse_property_description_data
from lib.tooling.schema import SchemaController, SchemaDiscovery, Command

@dataclass
class Quantile:
    size: int
    start: date
    end: date

@dataclass
class NswLsrPropDescCommand:
    legal_descriptions: Quantile
    legal_description_by_strata_lot: Quantile

_logger = getLogger(__name__)

async def process_property_description(db: DatabaseService,
                                       io: IoService,
                                       workers: int) -> None:
    controller = SchemaController(io, db, SchemaDiscovery.create(io))

    async with db.async_connect() as c, c.cursor() as cursor:
        a, b = await asyncio.gather(
            find_table_quantiles(cursor, 'nsw_lrs.legal_description', workers),
            find_table_quantiles(cursor, 'nsw_lrs.legal_description_by_strata_lot', workers),
        )

    commands = [
        NswLsrPropDescCommand(a_v, b[a_k])
        for a_k, a_v in a.items()
    ]
    tasks = [asyncio.create_task(worker(db, c)) for c in commands]
    _logger.info("Running nsw_lrs.legal_description ingestion")
    await asyncio.gather(*tasks)

async def find_table_quantiles(cursor, table, quantiles) -> Dict[int, Quantile]:
    await cursor.execute(f"""
        SELECT segment, MIN(effective_date), MAX(effective_date), COUNT(*)
        FROM (SELECT effective_date, NTILE({quantiles}) OVER (ORDER BY effective_date) AS segment
                FROM {table}
                LEFT JOIN meta.source_byte_position USING (source_id)
                LEFT JOIN meta.file_source USING (file_source_id)
               WHERE date_published >= '2004-08-18') t
        GROUP BY segment
        ORDER BY segment
    """)
    return {
        row[0]: Quantile(row[3], row[1], row[2])
        for row in await cursor.fetchall()
    }

async def worker(db: DatabaseService, task: NswLsrPropDescCommand) -> None:
    await chunk_quatile(db, 'nsw_lrs.legal_description', task.legal_descriptions)

async def chunk_quatile(db: DatabaseService, table_name: str, q: Quantile) -> None:
    limit = 100
    temp_table_name = f"q_{uuid.uuid4().hex[:8]}"

    async with db.async_connect() as conn, conn.cursor() as cursor:
        await cursor.execute(f"""
            SET session_replication_role = 'replica';

            CREATE TEMP TABLE pg_temp.{temp_table_name} AS
            SELECT legal_description_id
              FROM nsw_lrs.legal_description
              LEFT JOIN meta.source_byte_position USING (source_id)
              LEFT JOIN meta.file_source USING (file_source_id)
             WHERE effective_date >= '{q.start}'
               AND effective_date < '{q.end}'
               AND legal_description_kind = '> 2004-08-17';
        """)

        for offset in range(0, q.size, limit):
            await cursor.execute(f"""
                SELECT source_id, legal_description, property_id, effective_date
                  FROM pg_temp.{temp_table_name}
                  LEFT JOIN nsw_lrs.legal_description USING (legal_description_id)
                  LEFT JOIN meta.source_file_line USING (source_id)
                  LEFT JOIN meta.file_source USING (file_source_id)
                  LIMIT {limit} OFFSET {offset}
            """)

            for source, description, property, effective_date in await cursor.fetchall():
                property_desc = parse_property_description_data(description)

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
                        for parcel in property_desc.parcels.all
                    ])

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
                        for parcel_id, partial in [
                            *((p.id, True) for p in property_desc.parcels.partial),
                            *((p.id, False) for p in property_desc.parcels.complete),
                        ]
                    ])

                except Exception as e:
                    _logger.error(f'chunk_quatile, failed with {description}')
                    raise e

        await cursor.execute(f"""
            DROP TABLE pg_temp.{temp_table_name};
            SET session_replication_role = 'origin';
        """)

