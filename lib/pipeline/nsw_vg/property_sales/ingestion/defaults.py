from .config import *

CONFIG = IngestionConfig(
    schema='nsw_vg_raw',
    tables=IngestionTableMap(
        a=IngestionTableConfig(
            table='property_sale_data_row_a_modern',
        ),
        b=IngestionTableConfig(
            table='property_sale_data_row_b_modern',
        ),
        c=IngestionTableConfig(
            table='property_sale_data_row_c',
        ),
        d=IngestionTableConfig(
            table='property_sale_data_row_d',
        ),
        a_legacy=IngestionTableConfig(
            table='property_sale_data_row_a_legacy',
        ),
        b_legacy=IngestionTableConfig(
            table='property_sale_data_row_b_legacy',
        ),
    ),
)
