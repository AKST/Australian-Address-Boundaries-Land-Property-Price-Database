import pandas as pd
from logging import getLogger
from typing import Self, List

from lib.service.database import DatabaseService
from lib.pipeline.nsw_lrs.property_description.parse import parse_land_parcel_ids, types

# TODO implement properly #42
async def process_property_description(db: DatabaseService) -> None:
    async with db.async_connect() as c, c.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS nsw_valuer_general.land_parcel_link")
        with open('sql/nsw_lv_schema_3_property_description_meta_data.sql', 'r') as f:
            await cursor.execute(f.read())

    def land_parcels(desc: str) -> List[types.LandParcel]:
        desc, parcels = parse_land_parcel_ids(desc)
        return parcels

    logger = getLogger(f'{__name__}.process_property_description')
    engine = db.engine()
    query = "SELECT * FROM nsw_lrs.property_description"
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

    for t in ['property', 'land_parcel_link']:
        query = f'SELECT COUNT(*) FROM nsw_lrs.{t}'
        count = pd.read_sql(query, engine)
        logger.info(f'COUNT(nsw_lrs.{t}) = {count.iloc[0, 0]}')


