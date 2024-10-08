if __name__ == '__main__':
    import asyncio
    import logging
    import pprint

    from lib.service.io import IoService
    from lib.service.database import DatabaseService
    from lib.service.database.defaults import DB_INSTANCE_2_CONFIG
    from lib.tooling.schema.controller import SchemaController
    from lib.tooling.schema.discovery import SchemaDiscovery
    from lib.tooling.schema.type import *

    logging.basicConfig(
        level=logging.DEBUG,
        format='[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    async def main() -> None:
        io = IoService.create(None)
        db = DatabaseService.create(DB_INSTANCE_2_CONFIG, 1)
        try:
            await db.open()
            discovery = SchemaDiscovery.create(io)
            controller = SchemaController(io, db, discovery)
            await controller.command(Command.Drop(ns='abs'))
            await controller.command(Command.Create(ns='abs', omit_foreign_keys=True))
            await controller.command(Command.AddForeignKeys(ns='abs'))
            await controller.command(Command.RemoveForeignKeys(ns='abs'))
            await controller.command(Command.Drop(ns='abs'))
            await controller.command(Command.Create(ns='abs'))
            await controller.command(Command.Truncate(ns='abs', cascade=True))
        finally:
            await db.close()

    asyncio.run(main())
