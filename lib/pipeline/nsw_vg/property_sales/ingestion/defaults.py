from .config import *

NSW_VG_PS_INGESTION_CONFIG = IngestionConfig(
    schema='nsw_vg_raw',
    tables=IngestionTableMap(
        a_legacy=IngestionTableConfig(
            table='ps_row_a_legacy',
            uniques=['file_path'],
        ),
        b_legacy=IngestionTableConfig(
            table='ps_row_b_legacy',
            uniques=['file_path', 'position'],
        ),
        a=IngestionTableConfig(
            table='ps_row_a',
            uniques=['file_path'],
        ),
        b=IngestionTableConfig(
            table='ps_row_b',
            uniques=['file_path', 'position'],
        ),
        c=IngestionTableConfig(
            table='ps_row_c',
            uniques=['file_path', 'position'],
        ),
        d=IngestionTableConfig(
            table='ps_row_d',
            uniques=['file_path', 'position'],
        ),
    ),
)
