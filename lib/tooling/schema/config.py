from typing import List, Set

from .type import SchemaNamespace

ns_dependency_order: List[SchemaNamespace] = [
    'meta',
    'abs',
    'nsw_lrs',
    'nsw_gnb',
    'nsw_planning',
    'nsw_spatial',
    'nsw_vg',
    'gnaf',
]

schema_ns: Set[SchemaNamespace] = {
    'abs',
    'meta',
    'nsw_lrs',
    'nsw_gnb',
    'nsw_planning',
    'nsw_spatial',
    'nsw_vg',
    'gnaf',
}

