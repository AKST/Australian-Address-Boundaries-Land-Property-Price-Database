from .config import *

NSW_VG_PS_INGESTION_CONFIG = IngestionConfig(
    schema='nsw_vg_raw',
    tables=IngestionTableMap(
        a_legacy=IngestionTableConfig(
            table='ps_row_a_legacy',
        ),
        b_legacy=IngestionTableConfig(
            table='ps_row_b_legacy',
        ),
        a=IngestionTableConfig(
            table='ps_row_a',
        ),
        b=IngestionTableConfig(
            table='ps_row_b',
        ),
        c=IngestionTableConfig(
            table='ps_row_c',
        ),
        d=IngestionTableConfig(
            table='ps_row_d',
        ),
    ),
)
