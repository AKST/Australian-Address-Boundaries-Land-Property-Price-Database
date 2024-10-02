import logging

from lib.service.io import IoService
from lib.service.database import DatabaseService

from .discovery import GnafPublicationTarget

_logger = logging.getLogger(__name__)

# TODO get rid of this and move logic to other script
async def init_target_schema(
    gnaf_target: GnafPublicationTarget,
    io: IoService,
    db: DatabaseService,
):
    for script in [
        gnaf_target.create_tables_sql,
        gnaf_target.fk_constraints_sql,
        'sql/move_gnaf_to_schema.sql',
    ]:
        _logger.info(f"running {script}")
        async with await db.async_connect() as c, c.cursor() as cursor:
            await cursor.execute(await io.f_read(script))
