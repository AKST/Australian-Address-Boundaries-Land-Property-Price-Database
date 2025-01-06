from dataclasses import dataclass, field
from os.path import join as join_path
from sqlglot import Expression
from typing import Dict, List, Literal, Optional, Self

SchemaNamespace = Literal[
    'abs',
    'meta',
    'nsw_lrs',
    'nsw_planning',
    'nsw_spatial',
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
        run_raw_schema: bool = field(default=False)

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
    class OpaqueDoBlock(Op):
        pass

    @dataclass
    class CreateSchema(Op):
        schema_name: str

    @dataclass
    class CreateTable(Op):
        schema_name: Optional[str]
        table_name: str

    @dataclass
    class CreateTablePartition(Op):
        schema_name: Optional[str]
        paritition_name: str

    @dataclass
    class CreateView(Op):
        schema_name: Optional[str]
        view_name: str
        materialized: bool

    @dataclass
    class CreateType(Op):
        schema_name: Optional[str]
        type_name: str

    @dataclass
    class CreateFunction(Op):
        schema_name: Optional[str]
        type_name: str

    @dataclass
    class CreateIndex(Op):
        index_name: str

        @property
        def is_concurrent(self: Self) -> bool:
            return self.expr_tree.args['concurrently']

@dataclass
class SchemaSyntax:
    expr_tree: List[Expression] = field(repr=False)
    operations: List[Stmt.Op]

    @property
    def can_be_used_in_transaction(self: Self) -> bool:
        return not any(
            operation.is_concurrent
            for operation in self.operations
            if isinstance(operation, Stmt.CreateIndex)
        )

@dataclass
class SqlFileMetaData:
    file_name: str
    root_dir: str
    ns: SchemaNamespace
    step: int
    name: Optional[str]
    contents: Optional[SchemaSyntax]

    @property
    def is_known_to_be_transaction_unsafe(self: Self) -> bool:
        match self.contents:
            case None:
                return False
            case contents:
                return not self.contents.can_be_used_in_transaction

    def path(self: Self) -> str:
        step_s = f'{self.step:03}'
        suffix = f'_{self.name}' if self.name else f''
        f_name = f'{step_s}_APPLY{suffix}.sql'
        return join_path(self.root_dir, self.ns, 'schema', f_name)

SchemaSteps = Dict[SchemaNamespace, List[SqlFileMetaData]]
