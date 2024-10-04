import geopandas as gpd
import logging
import pandas as pd

from lib.service.database import DatabaseService

from .config import IngestionConfig

_logger = logging.getLogger(__name__)

async def ingest(db: DatabaseService, config: IngestionConfig, outdir: str) -> None:
    async with db.async_connect() as c, c.cursor() as cursor:
        # this really won't do anything unless you need to rerun this portion of the script
        for table in config.tables():
            await cursor.execute(f"""
            DO $$ BEGIN
              IF EXISTS (
                SELECT 1 FROM information_schema.tables
                 WHERE table_name = '{table}' AND table_schema = '{config.schema}'
              ) THEN
                TRUNCATE TABLE {config.schema}.{table} RESTART IDENTITY CASCADE;
              END IF;
            END $$;
            """)

        with open(config.create_table_script, 'r') as f:
            await cursor.execute(f.read())

    engine = db.engine()

    for layer_name, table_name in config.layer_to_table.items():
        column_renames = config.database_column_names_for_dataframe_columns[layer_name]

        df = gpd.read_file(f'{outdir}/{config.gpkg_export_path}', layer=layer_name)
        df = df.rename(columns=column_renames)
        df = df[list(column_renames.values())]

        if 'in_australia' in df:
            df['in_australia'] = df['in_australia'] == 'AUS'

        df.to_postgis(table_name,
                      engine,
                      schema=config.schema,
                      if_exists='append',
                      index=False)

        with engine.connect() as connection:
            result = pd.read_sql(f"SELECT COUNT(*) FROM {config.schema}.{table_name}", connection)
            _logger.info(f"Populated {config.schema}.{table_name} with {result.iloc[0, 0]}/{len(df)} rows.")

