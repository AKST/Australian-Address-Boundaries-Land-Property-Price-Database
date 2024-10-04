from dataclasses import dataclass, field
from logging import getLogger
from typing import Self

from lib.pipeline.nsw_vg.property_sales import data as t

@dataclass
class IngestionTableConfig:
    table: str
    schema: str | None = field(default=None)

    @property
    def table_symbol(self: Self) -> str:
        if self.schema:
            return f'{self.schema}.{self.table}'
        return self.table

    def hydrate(self: Self, schema: str | None) -> 'IngestionTableConfig':
        schema = self.schema if self.schema else schema
        return IngestionTableConfig(table=self.table,
                                    schema=schema)

@dataclass
class IngestionTableMap:
    a_legacy: IngestionTableConfig
    b_legacy: IngestionTableConfig
    a: IngestionTableConfig
    b: IngestionTableConfig
    c: IngestionTableConfig
    d: IngestionTableConfig

    def get_config(self: Self, row: t.BasePropertySaleFileRow) -> IngestionTableConfig | None:
        match row:
            case t.SaleRecordFile():
                return self.a
            case t.SaleRecordFileLegacy():
                return self.a_legacy
            case t.SalePropertyDetails():
                return self.b
            case t.SalePropertyDetails1990():
                return self.b_legacy
            case t.SalePropertyLegalDescription():
                return self.c
            case t.SaleParticipant():
                return self.d
            case t.SaleDataFileSummary():
                return None
            case t.BasePropertySaleFileRow():
                raise ValueError('this shouldn\'t happen')
        raise ValueError(f'unknown row, {row}')

@dataclass
class IngestionConfig:
    schema: str
    tables: IngestionTableMap

    def get_config(self: Self, row: t.BasePropertySaleFileRow) -> IngestionTableConfig:
        match self.tables.get_config(row):
            case None: raise ValueError('unexpected row type')
            case conf: return conf.hydrate(schema=self.schema)
