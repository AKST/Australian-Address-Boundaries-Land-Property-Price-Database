from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Any, List, Literal, Dict, Union, Tuple

SdmxDataDetail = Literal['full', 'dataonly', 'serieskeysonly', 'nodata']
SdmxMetaDetail = Literal[
        'allstubs', 'referencestubs', 'referencepartial',
        'allcompletestubs', 'referencecompletestubs', 'full']

DimensionAtObservation = Union[
    Literal['TIME_PERIOD','AllDimensions'],
    'DataStructureMeta.Dimension',
]

# https://data.api.abs.gov.au/rest/codelist/ABS/CL_FREQ?detail=full
Frequency = Literal['H', 'D', 'N', 'B', 'W', 'S', 'A', 'M', 'Q']

StructureType = Literal[
    'datastructure', 'dataflow', 'codelist', 'conceptscheme',
    'categoryscheme', 'contentconstraint', 'actualconstraint',
    'agencyscheme', 'categorisation', 'hierarchicalcodelist']

DataKey = List[Union[str, List[str]]]

@dataclass
class DataFlowMeta:
    @dataclass
    class DataFlowParam:
        id: str
        default_value: str

    id: str
    version: str
    name: Optional[str]
    desc: Optional[str]
    params: List[DataFlowParam]
    layout_col: List[str]
    layout_row: List[str]
    layout_row_sec: List[str]
    constraints: Optional['ContentConstraintsMeta'] = field(repr=False)
    raw: Any = field(repr=False)

@dataclass
class ContentConstraintsMeta:
    @dataclass
    class CubeRegion:
        id: str

    @dataclass
    class EnumeratedCubeRegion(CubeRegion):
        values: List[str]

    @dataclass
    class TimeCubeRegion(CubeRegion):
        start: datetime
        start_inclusive: bool
        end: datetime
        end_inclusive: bool

    id: str
    version: str
    name: str
    cube_regions: List[CubeRegion]
    raw: Any = field(repr=False)

@dataclass
class CodeListMeta:
    @dataclass
    class CodeListEntry:
        id: str
        name: Optional[str]
        order: Optional[str]
        further_information: Optional[str]

    id: str
    name: Optional[str]
    version: str
    codes: List[CodeListEntry]
    raw: Any = field(repr=False)

@dataclass
class ConceptSchemeMeta:
    @dataclass
    class Concept:
        id: str
        name: Optional[str]

    id: str
    version: str
    name: Optional[str]
    concepts: List[Concept]
    raw: Any


@dataclass
class CategorisationMeta:
    id: str
    name: Optional[str]
    raw: Any

TextType = Literal['ObservationalTimePeriod', 'Double']
DimensionType = Literal['Dimension', 'TimeDimension']
AssignmentStatus = Literal['Conditional', 'Mandatory']
ComponentRelation = Literal['dimensions', 'primaryMeasure']

@dataclass
class DataStructureMeta:
    class LocalRepr:
        pass

    @dataclass
    class LocalReprEnumeration(LocalRepr):
        enumeration: str

    @dataclass
    class LocalReprTextFormat(LocalRepr):
        is_multi_lingual: bool
        is_sequence: bool
        text_type: TextType

    @dataclass
    class PrimaryMeasure:
        id: str
        concept_identity: str
        local_representation: Optional['DataStructureMeta.LocalRepr'] = field(repr=False)

    @dataclass
    class Dimension:
        id: str
        type: DimensionType = field(repr=False)
        position: int = field(repr=False)
        concept_identity: str
        local_representation: Optional['DataStructureMeta.LocalRepr'] = field(repr=False)

    @dataclass
    class AttributeRelations:
        primary_measure: Optional[str]
        dimensions: List[str]

    @dataclass
    class Attribute:
        id: str
        assignment_status: AssignmentStatus
        concept_identity: str
        relationships: Optional['DataStructureMeta.AttributeRelations']
        local_representation: Optional['DataStructureMeta.LocalRepr'] = field(repr=False)

    id: str
    name: Optional[str]
    version: Optional[str]
    attributes: List[Attribute]
    dimensions: Dict[str, Dimension] = field(repr=False)
    dimension_datakey: List[Dimension]
    dimension_time: Optional[Dimension]
    primary_measure: PrimaryMeasure
    raw: Any = field(repr=False)

class ObservationMeta:
    @dataclass
    class DimensionValue:
        id: str
        name: str
        # annotations: List[str]

    @dataclass
    class DimensionEnumableValue(DimensionValue):
        order: int
        parent: Optional[str]

    @dataclass
    class DimensionTemporalValue(DimensionValue):
        start: datetime
        end: datetime

    @dataclass
    class DimensionDefinition:
        id: str
        name: str
        pos: int
        values: List['ObservationMeta.DimensionValue']

    @dataclass
    class DimensionLookupTable:
        urn: str
        dimensions: List['ObservationMeta.DimensionDefinition']
        raw: Any = field(repr=False)

    @dataclass
    class DimensionRef:
        index: Tuple[int, int]
        value: 'ObservationMeta.DimensionValue'

class Observation:
    @dataclass
    class AllDimensions:
        urn: str = field(repr=False)
        count: int
        dimension_meta: Dict[str, ObservationMeta.DimensionRef]
        _table: ObservationMeta.DimensionLookupTable

        @property
        def dimensions(self) -> Dict[str, str]:
            return { k: d.value.id for k, d in self.dimension_meta.items() }

        def __repr__(self) -> str:
            dimensions = list(self.dimensions.values())
            return f'Observation.AllDimensions(count={self.count}, dimensions={dimensions})'

