from lib.service.database import DatabaseService
from .parse import parse_land_parcel_ids

async def process_property_description(db: DatabaseService) -> None:
    async with db.async_connect() as c, c.cursor() as cursor:
        await cursor.execute(f'SELECT COUNT(*) FROM nsw_lrs.legal_description')
        propd_count = (await cursor.fetchone())[0]
        await cursor.execute(f'SELECT COUNT(*) FROM nsw_lrs.legal_description_by_strata_lot')
        stata_propd_count = (await cursor.fetchone())[0]
        print(propd_count, stata_propd_count)
