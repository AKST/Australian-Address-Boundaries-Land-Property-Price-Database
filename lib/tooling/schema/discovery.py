import re
from dataclasses import dataclass
from logging import getLogger
from os.path import relpath, basename
from sqlglot import parse as parse_sql, Expression
import sqlglot.expressions as sql_expr
from typing import cast, List, Optional, Set, Self, Type

from lib.service.io import IoService
from lib.utility.concurrent import fmap
from lib.utility.iteration import partition

from .config import schema_ns
from .type import *

@dataclass
class _FileMeta:
    ns: SchemaNamespace
    step: int
    name: Optional[str]

class SchemaDiscovery:
    logger = getLogger(f'{__name__}.SchemaDiscovery')
    file_regex: re.Pattern
    root_dir: str
    _io: IoService

    def __init__(self: Self,
                 root_dir: str,
                 file_regex: re.Pattern[str],
                 io: IoService) -> None:
        self.root_dir = root_dir
        self.file_regex = file_regex
        self._io = io

    @staticmethod
    def create(io: IoService, root_dir: Optional[str] = None) -> 'SchemaDiscovery':
        root_dir = basename('./sql' if root_dir is None else root_dir)
        path_root = re.escape(root_dir)
        path_ns = r'(?P<ns>[_a-zA-Z][_a-zA-Z0-9]*)'
        path_file = r'(?P<step>\d{3})_APPLY(_(?P<name>[_a-zA-Z][_a-zA-Z0-9]*))?.sql'
        pattern = re.compile(rf'^{path_root}/{path_ns}/schema/{path_file}$')
        return SchemaDiscovery(root_dir, pattern, io)

    async def files(
        self: Self,
        name: SchemaNamespace,
        maybe_range: Optional[range] = None,
        load_syn=False,
    ) -> List[SqlFileMetaData]:
        metas = [(f, self.__f_meta_data(f)) for f in await self.__ns_sql(name)]

        return sorted([
            await self.__f_sql_meta_data(f, meta, load_syn)
            for f, meta in metas
            if maybe_range is None or meta.step in maybe_range
        ], key=lambda it: it.step)


    async def all_files(
            self: Self,
            names: Optional[Set[SchemaNamespace]] = None,
            load_syn=False,
    ) -> SchemaSteps:
        return {
            namespace: sorted([
                await self.__f_sql_meta_data(f, meta, load_syn)
                for f, meta in [
                    (f, self.__f_meta_data(f))
                    for f in await self.__ns_sql(namespace)
                ]
                for f in await self.__ns_sql(namespace)
            ], key=lambda it: it.step)
            for namespace in (names or schema_ns)
        }

    async def __ns_sql(self: Self, ns: SchemaNamespace) -> List[str]:
        glob_s = '*_APPLY*.sql'
        root_d = f'{self.root_dir}/{ns}/schema'
        return [f async for f in self._io.grep_dir(root_d, glob_s)]

    def __f_meta_data(self: Self, f: str) -> _FileMeta:
        match self.file_regex.match(f):
            case None: raise ValueError(f'invalid file {f}')
            case match:
                ns_str = match.group('ns')
                step = int(match.group('step'))
                name = match.group('name')
                if ns_str not in schema_ns:
                    raise TypeError(f'unknown namespace {ns_str}')
                ns: SchemaNamespace = cast(SchemaNamespace, ns_str)
                return _FileMeta(ns, step, name)

    async def __f_sql_meta_data(self: Self, f: str, meta: _FileMeta, load_syn: bool) -> SqlFileMetaData:
        try:
            contents = await fmap(sql_as_operations, self._io.f_read(f)) if load_syn else None
            return SqlFileMetaData(self.root_dir, meta.ns, meta.step, meta.name, contents)
        except Exception as e:
            self.logger.error(f'failed on {f}')
            raise e

def sql_as_operations(file_data: str) -> SchemaSyntax:
    sql_exprs = parse_sql(file_data, read='postgres')
    generator_a = ((expr, expr_as_op(expr)) for expr in sql_exprs if expr)
    generator_b = [(expr, op) for expr, op in generator_a if op is not None]
    return SchemaSyntax(*partition(generator_b))

def expr_as_op(expr: Expression) -> Optional[Stmt.Op]:
    match expr:
        case sql_expr.Create(kind="SCHEMA", this=schema_def):
            s_name = schema_def.db
            return Stmt.CreateSchema(expr, s_name)
        case sql_expr.Create(kind="TABLE", this=schema):
            t_name = schema.this.this.this
            s_name = schema.this.db or None
            return Stmt.CreateTable(expr, s_name, t_name)
        case sql_expr.Create(kind="INDEX", this=schema):
            t_name = schema.this.this
            return Stmt.CreateIndex(expr, t_name)
        case sql_expr.Command(this="CREATE", expression=e):
            match re.findall(f'\w+', e.lower()):
                case ['type', s_name, t_name, 'as', *_]:
                    return Stmt.CreateType(expr, s_name, t_name)
                case ['type', t_name, 'as', *_]:
                    return Stmt.CreateType(expr, None, t_name)
                case other:
                    raise TypeError(f'unknown command {repr(other)}')
        case sql_expr.Semicolon():
            # this typically means a comment
            return None
        case other:
            raise TypeError(f'unknown {repr(other)}')


