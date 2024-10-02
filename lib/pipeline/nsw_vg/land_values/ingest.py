import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logging import getLogger
import math
import pandas as pd
import os
from typing import Self, List

from lib.service.database import DatabaseService
from lib.pipeline.nsw_vg.discovery import NswVgTarget
from lib.pipeline.nsw_vg.property_description import parse_land_parcel_ids
from lib.pipeline.nsw_vg.property_description.types import LandParcel

from .constants import *

_SCHEMA = 'nsw_valuer_general'

class _Ctrl:
    def __init__(self, db: DatabaseService, logger_name: str) -> None:
        self.db = db
        self._logger = getLogger(f'{__name__}.{logger_name}')

    def get_count(self, table_name: str) -> int:
        c = pd.read_sql(f"""
            SELECT count(*)
            FROM nsw_valuer_general.{table_name}
        """, self.db.engine())
        return c.iloc[0,0]

    def debug(self: Self, message: str):
        self._logger.debug(message)

    def log_count(self: Self, table_name: str) -> None:
        count = self.get_count(table_name)
        self._logger.info(f'{table_name} {count}')

async def ingest_raw_files(db: DatabaseService,
                     target: NswVgTarget,
                     read_dir: str) -> None:
    """
    Here we are just loading the each file from the latest
    land value publication with minimal changes, and a bit
    of sanitisizing.
    """
    async with await db.async_connect() as c, c.cursor() as cursor:
        await cursor.execute(f"DROP TABLE IF EXISTS {_SCHEMA}.raw_entries_lv CASCADE")
        with open('sql/nsw_lv_schema_1_raw.sql', 'r') as f:
            await cursor.execute(f.read())

    column_mappings = {
        **LV_LONG_COLUMN_MAPPINGS,
        **LV_WIDE_COLUMNS_MAPPINGS,
    }

    # TODO convert to use ASYNCIO
    def process_file(file: str, ctrl: _Ctrl) -> None:
        if not file.endswith("csv"):
            return

        full_file_path = f"{read_dir}/{target.zip_dst}/{file}"
        try:
            df = pd.read_csv(full_file_path, encoding='utf-8')
        except UnicodeDecodeError:
            # Fallback to ISO-8859-1 encoding if utf-8 fails
            df = pd.read_csv(full_file_path, encoding='ISO-8859-1')

        date_str = file.split('_')[-1].replace('.csv', '')

        df.index.name = 'source_file_position'
        df = df.drop(columns=['Unnamed: 34'])
        df = df.rename(columns=column_mappings).reset_index()
        df['source_file_name'] = file
        df['source_date'] = datetime.strptime(date_str, "%Y%m%d")
        df['postcode'] = [(n if math.isnan(n) else str(int(n))) for n in df['postcode']]

        try:
            df.to_sql('raw_entries_lv', db.engine(), schema='nsw_valuer_general', if_exists='append', index=False)
        finally:
            count = ctrl.get_count('raw_entries_lv')
            ctrl.debug(f'raw_entries_lv (total {count}) read {full_file_path}')

    files = sorted(os.listdir(f"_out_zip/{target.zip_dst}"))
    ctrl = _Ctrl(db, 'ingest_raw_files')

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(process_file, file, ctrl) for file in files]
        for future in as_completed(futures):
            future.result()

async def create_vg_tables_from_raw(db: DatabaseService) -> None:
    """
    Just to break up the data into more efficent
    representations of the data, and data that will be
    easier to query, we're going to perform a series of
    queries against the GNAF data before using it
    populate the tables we care about.
    """

    async with await db.async_connect() as c, c.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.source_file CASCADE")
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.source CASCADE")
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.district CASCADE")
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.suburb CASCADE")
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.street CASCADE")
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.property CASCADE")
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.property_description CASCADE")
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.valuations CASCADE")

        with open('sql/nsw_lv_schema_2_structure.sql', 'r') as f:
            await cursor.execute(f.read())

        with open('sql/nsw_lv_from_raw.sql', 'r') as f:
            await cursor.execute(f.read())

    ctrl = _Ctrl(db, 'create_vg_tables_from_raw')
    ctrl.log_count('district')
    ctrl.log_count('suburb')
    ctrl.log_count('street')
    ctrl.log_count('property')
    ctrl.log_count('property_description')
    ctrl.log_count('valuations')

async def parse_property_description(db: DatabaseService) -> None:
    async with await db.async_connect() as c, c.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.land_parcel_link")
        with open('sql/nsw_lv_schema_3_property_description_meta_data.sql', 'r') as f:
            await cursor.execute(f.read())

    def land_parcels(desc: str) -> List[LandParcel]:
        desc, parcels = parse_land_parcel_ids(desc)
        return parcels

    engine = db.engine()
    query = "SELECT * FROM nsw_valuer_general.property_description"
    for df_chunk in pd.read_sql(query, engine, chunksize=10000):
        df_chunk = df_chunk.dropna(subset=['property_description'])
        df_chunk['parcels'] = df_chunk['property_description'].apply(land_parcels)
        df_chunk_ex = df_chunk.explode('parcels')
        df_chunk_ex = df_chunk_ex.dropna(subset=['parcels'])
        df_chunk_ex['land_parcel_id'] = df_chunk_ex['parcels'].apply(lambda p: p.id)
        df_chunk_ex['part'] = df_chunk_ex['parcels'].apply(lambda p: p.part)
        df_chunk_ex = df_chunk_ex.drop(columns=['property_description', 'parcels'])
        df_chunk_ex.to_sql(
            'land_parcel_link',
            con=engine,
            schema='nsw_valuer_general',
            if_exists='append',
            index=False,
        )

    ctrl = _Ctrl(db, 'parse_property_description')

    for t in ['property', 'land_parcel_link']:
        ctrl.log_count(t)

async def empty_raw_entries(db: DatabaseService) -> None:
    async with await db.async_connect() as c, c.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.raw_entries_lv")
