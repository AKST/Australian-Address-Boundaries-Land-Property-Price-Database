import geopandas as gpd
import pandas as pd

from lib.gnaf_db import GnafDb

from .config import IngestionConfig

def ingest_sync(gnaf_db: GnafDb,
                config: IngestionConfig,
                outdir: str) -> None:
    with gnaf_db.connect() as conn:
        cursor = conn.cursor()

        # this really won't do anything unless you need to rerun this portion of the script
        for table in config.tables():
            cursor.execute(f"""
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
            cursor.execute(f.read())

        cursor.close()

    engine = gnaf_db.engine()

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
            print(f"Populated {config.schema}.{table_name} with {result.iloc[0, 0]}/{len(df)} rows.")
