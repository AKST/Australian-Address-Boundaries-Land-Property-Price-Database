from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logging import getLogger
import math
import pandas as pd
import os
from typing import Self, List

from lib.gnaf_db import GnafDb
from lib.nsw_vg.discovery import NswVgTarget
from lib.nsw_vg.property_description import parse_land_parcel_ids
from lib.nsw_vg.property_description.types import LandParcel

from .constants import *

_SCHEMA = 'nsw_valuer_general'

class _Ctrl:
    def __init__(self, db: GnafDb, logger_name: str) -> None:
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

def ingest_raw_files(gnaf_db: GnafDb,
                     target: NswVgTarget,
                     read_dir: str) -> None:
    """
    Here we are just loading the each file from the latest
    land value publication with minimal changes, and a bit
    of sanitisizing.
    """
    with gnaf_db.connect() as conn:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {_SCHEMA}.raw_entries_lv CASCADE")
        with open('sql/nsw_lv_schema_1_raw.sql', 'r') as f:
            cursor.execute(f.read())
        cursor.close()

    column_mappings = {
        **LV_LONG_COLUMN_MAPPINGS,
        **LV_WIDE_COLUMNS_MAPPINGS,
    }

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
            df.to_sql('raw_entries_lv', gnaf_db.engine(), schema='nsw_valuer_general', if_exists='append', index=False)
        finally:
            count = ctrl.get_count('raw_entries_lv')
            ctrl.debug(f'raw_entries_lv (total {count}) read {full_file_path}')

    files = sorted(os.listdir(f"_out_zip/{target.zip_dst}"))
    ctrl = _Ctrl(gnaf_db, 'ingest_raw_files')

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = [executor.submit(process_file, file, ctrl) for file in files]
        for future in as_completed(futures):
            future.result()

def create_vg_tables_from_raw(gnaf_db: GnafDb) -> None:
    """
    Just to break up the data into more efficent
    representations of the data, and data that will be
    easier to query, we're going to perform a series of
    queries against the GNAF data before using it
    populate the tables we care about.
    """

    with gnaf_db.connect() as conn:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.source_file CASCADE")
        cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.source CASCADE")
        cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.district CASCADE")
        cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.suburb CASCADE")
        cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.street CASCADE")
        cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.property CASCADE")
        cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.property_description CASCADE")
        cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.valuations CASCADE")

        with open('sql/nsw_lv_schema_2_structure.sql', 'r') as f:
            cursor.execute(f.read())

        with open('sql/nsw_lv_from_raw.sql', 'r') as f:
            cursor.execute(f.read())

        cursor.close()

    ctrl = _Ctrl(gnaf_db, 'create_vg_tables_from_raw')
    ctrl.log_count('district')
    ctrl.log_count('suburb')
    ctrl.log_count('street')
    ctrl.log_count('property')
    ctrl.log_count('property_description')
    ctrl.log_count('valuations')

def parse_property_description(gnaf_db: GnafDb) -> None:
    engine = gnaf_db.engine()

    with gnaf_db.connect() as conn:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.land_parcel_link")
        with open('sql/nsw_lv_schema_3_property_description_meta_data.sql', 'r') as f:
            cursor.execute(f.read())
        cursor.close()

    def land_parcels(desc: str) -> List[LandParcel]:
        desc, parcels = parse_land_parcel_ids(desc)
        return parcels

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

    ctrl = _Ctrl(gnaf_db, 'parse_property_description')

    with gnaf_db.connect() as conn:
        cursor = conn.cursor()
        for t in ['property', 'land_parcel_link']:
            ctrl.log_count(t)
        cursor.close()

def empty_raw_entries(gnaf_db: GnafDb) -> None:
    with gnaf_db.connect() as conn:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.raw_entries_lv")
        cursor.close()
