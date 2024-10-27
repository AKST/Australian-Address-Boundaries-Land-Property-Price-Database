import logging
from typing import List

from lib.service.database import DatabaseService
from lib.tooling.schema.config import schema_ns
from lib.tooling.schema.type import SchemaNamespace

class Application:
    def __init__(self, db: DatabaseService) -> None:
        self.db = db

    async def count(self, namespace: str, table: str) -> int:
        async with self.db.async_connect() as c, c.cursor() as cursor:
            await cursor.execute(f'SELECT COUNT(*) FROM {namespace}.{table}')
            results = await cursor.fetchone()
            return results[0]

    async def tables(self, namespace: str) -> List[str]:
        async with self.db.async_connect() as c, c.cursor() as cursor:
            await cursor.execute(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = '{namespace}'
            """)
            return [it[0] for it in await cursor.fetchall()]

async def run_count_for_schemas(db_conf, packages: List[SchemaNamespace]):
    db = DatabaseService.create(db_conf, 1)
    app = Application(db)
    logger = logging.getLogger(f'{__name__}.count')

    logger.info('# Row Count')
    for pkg in packages:
        tables = await app.tables(pkg)
        for tlb in tables:
            count = await app.count(pkg, tlb)
            logger.info(f' - "{pkg}.{tlb}" {count} rows')

if __name__ == '__main__':
    import asyncio
    import argparse
    import resource

    from lib.service.database.defaults import DB_INSTANCE_MAP

    parser = argparse.ArgumentParser(description="db schema tool")
    parser.add_argument("--debug", action='store_true', default=False)
    parser.add_argument("--instance", type=int, required=True)
    parser.add_argument("--packages", nargs='*', required=True)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    db_conf = DB_INSTANCE_MAP[args.instance]
    packages = [s for s in schema_ns if s in args.packages]
    asyncio.run(run_count_for_schemas(db_conf, packages))

