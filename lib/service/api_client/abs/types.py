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
        local_representation: Optional['DataStructureMeta.LocalRepr']

    @dataclass
    class Dimension:
        id: str
        type: DimensionType
        concept_identity: str
        local_representation: Optional['DataStructureMeta.LocalRepr']

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
        local_representation: Optional['DataStructureMeta.LocalRepr']

    id: str
    structure_url: Optional[str]
    name: Optional[str]
    version: Optional[str]
    attributes: List[Attribute]
    dimensions: Dict[str, Dimension]
    primary_measure: PrimaryMeasure
    raw: Any = field(repr=False)



