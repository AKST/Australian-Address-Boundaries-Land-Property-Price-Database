from dataclasses import dataclass, field
from os.path import join as join_path
from sqlglot import Expression
from typing import Dict, List, Literal, Optional, Self

SchemaNamespace = Literal[
    'abs',
    'meta',
    'nsw_lrs',
    'nsw_planning',
    'nsw_gnb',
    'nsw_vg',
]

class Command:
    @dataclass
    class BaseCommand:
        ns: SchemaNamespace
        range: Optional[range] = field(default=None)
        dryrun: bool = field(default=False)

    @dataclass
    class Create(BaseCommand):
        omit_foreign_keys: bool = field(default=False)

    @dataclass
    class AddForeignKeys(BaseCommand):
        pass

    @dataclass
    class RemoveForeignKeys(BaseCommand):
        pass

    @dataclass
    class Drop(BaseCommand):
        cascade: bool = field(default=False)

    @dataclass
    class Truncate(BaseCommand):
        cascade: bool = field(default=False)

class Stmt:
    @dataclass
    class Op:
        expr_tree: Expression = field(repr=False)

    @dataclass
    class CreateSchema(Op):
        schema_name: str

    @dataclass
    class CreateTable(Op):
        schema_name: Optional[str]
        table_name: str

    @dataclass
    class CreateType(Op):
        schema_name: Optional[str]
        type_name: str

    @dataclass
    class CreateIndex(Op):
        index_name: str

@dataclass
class SchemaSyntax:
    expr_tree: List[Expression] = field(repr=False)
    operations: List[Stmt.Op]

@dataclass
class SqlFileMetaData:
    root_dir: str
    ns: SchemaNamespace
    step: int
    name: Optional[str]
    contents: Optional[SchemaSyntax]

    def path(self: Self) -> str:
        step_s = f'{self.step:03}'
        suffix = f'_{self.name}' if self.name else f''
        f_name = f'{step_s}_APPLY{suffix}.sql'
        return join_path(self.root_dir, self.ns, 'schema', f_name)

SchemaSteps = Dict[SchemaNamespace, List[SqlFileMetaData]]
