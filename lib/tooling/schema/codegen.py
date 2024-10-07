from typing import List, Iterator, Optional

from .type import *

def _id(schema: Optional[str], name: str) -> str:
    match schema:
        case None: return name
        case schema: return f'{schema}.{name}'

def drop(commands: SchemaSyntax, cascade: bool = False) -> Iterator[str]:
    sfx = ' CASCADE' if cascade else ''

    for operation in reversed(commands.operations):
        match operation:
            case Stmt.CreateSchema(expr, schema_name):
                yield f'DROP SCHEMA IF EXISTS {schema_name}{sfx}'
            case Stmt.CreateType(expr, schema_name, name):
                yield f'DROP TYPE IF EXISTS {_id(schema_name, name)}{sfx}'
            case Stmt.CreateTable(expr, schema_name, name):
                yield f'DROP TABLE IF EXISTS {_id(schema_name, name)}{sfx}'
            case Stmt.CreateIndex(expr, name):
                yield f'DROP INDEX IF EXISTS {name}'
            case other:
                raise TypeError(f'have not handled {other}')

def truncate(commands: SchemaSyntax, cascade: bool = False) -> Iterator[str]:
    sfx = ' CASCADE' if cascade else ''

    for operation in reversed(commands.operations):
        match operation:
            case Stmt.CreateSchema(expr, schema_name):
                continue
            case Stmt.CreateType(expr, schema_name, name):
                continue
            case Stmt.CreateTable(expr, schema_name, name):
                yield f'TRUNCATE TABLE {_id(schema_name, name)}{sfx}'
            case Stmt.CreateIndex(expr, name):
                continue
            case other:
                raise TypeError(f'have not handled {other}')
