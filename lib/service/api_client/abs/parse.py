from datetime import datetime
from typing import Set, Dict, Tuple, Optional
from .types import (
    ConceptSchemeMeta,
    ContentConstraintsMeta,
    DataFlowMeta,
    CodeListMeta,
    DataStructureMeta,
)

def parse_annotations(d, supported_types: Set[str], value_key: str) -> Dict[str, str]:
    if 'annotations' not in d:
        return {}

    return {
        v['type']: v[value_key]
        for v in d['annotations']
        if 'type' in v and v['type'] in supported_types and value_key in v
    }

def parse_conceptscheme_meta(d) -> ConceptSchemeMeta:
    def parse_concept(d) -> ConceptSchemeMeta.Concept:
        return ConceptSchemeMeta.Concept(d['id'], d.get('name'))

    return ConceptSchemeMeta(
        id=d['id'],
        version=d['version'],
        name=d.get('name'),
        concepts=[parse_concept(c) for c in d['concepts']],
        raw=d,
    )

def parse_contentconstraints_meta(d) -> ContentConstraintsMeta:
    time_format = "%Y-%m-%dT%H:%M:%S"
    def parse_cuberegion(kv) -> ContentConstraintsMeta.CubeRegion:
        match kv:
            case { 'id': id, 'values': values }:
                return ContentConstraintsMeta.EnumeratedCubeRegion(id, values)

            case { 'id': id, 'timeRange': timeRange }:
                return ContentConstraintsMeta.TimeCubeRegion(
                    id,
                    datetime.strptime(timeRange['startPeriod']['period'], time_format),
                    timeRange['startPeriod']['isInclusive'],
                    datetime.strptime(timeRange['endPeriod']['period'], time_format),
                    timeRange['endPeriod']['isInclusive'],
                )
        raise TypeError()

    return ContentConstraintsMeta(
        id=d['id'],
        version=d['version'],
        name=d['name'],
        cube_regions=[
            parse_cuberegion(kv)
            for cr in d['cubeRegions']
            for kv in cr['keyValues']
        ]
    )

def parse_dataflow_meta(d, data=None) -> DataFlowMeta:
    df_urn = f'urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=ABS:{d['id']}({d['version']})'
    annotations = parse_annotations(d, {'DEFAULT', 'LAYOUT_COLUMN', 'LAYOUT_ROW', 'LAYOUT_ROW_SECTION'}, 'title')
    params = [] if 'DEFAULT' not in annotations else [
        DataFlowMeta.DataFlowParam(p[0], p[1])
        for p in map(lambda it: it.split('='), annotations['DEFAULT'].split(','))]

    split = lambda s: s.split(',') if s else []

    constraints = None
    if data:
        constraints = next((
            parse_contentconstraints_meta(c)
            for c in data.get('contentConstraints', [])
            if df_urn in c['constraintAttachment']['dataflows']
        ), None)

    return DataFlowMeta(
        d['id'],
        d['version'],
        d['name'],
        d.get('description'),
        params,
        split(annotations.get('LAYOUT_COLUMN')),
        split(annotations.get('LAYOUT_ROW')),
        split(annotations.get('LAYOUT_ROW_SECTION')),
        constraints,
        d)

def parse_codelist_meta(d) -> CodeListMeta:
    def parse_code(code) -> CodeListMeta.CodeListEntry:
        annotations = parse_annotations(code, {'ORDER', 'FURTHER_INFORMATION'}, 'text')
        return CodeListMeta.CodeListEntry(
            code['id'],
            code['name'],
            annotations.get('ORDER'),
            annotations.get('FURTHER_INFORMATION'),
        )

    codes = [parse_code(c) for c in d['codes']]
    return CodeListMeta(d['id'], d['name'], d['version'], codes, d)

def parse_datastructure_meta(d, data=None) -> DataStructureMeta:
    def parse_local_representation(d) -> Optional[DataStructureMeta.LocalRepr]:
        match d:
            case None:
                return None
            case { 'enumeration': enumeration }:
                return DataStructureMeta.LocalReprEnumeration(enumeration)
            case { 'textFormat': text_fmt }:
                return DataStructureMeta.LocalReprTextFormat(
                    is_multi_lingual=text_fmt['isMultiLingual'],
                    is_sequence=text_fmt['isSequence'],
                    text_type=text_fmt['textType'])
        raise TypeError()

    def parse_measure(d) -> DataStructureMeta.PrimaryMeasure:
        return DataStructureMeta.PrimaryMeasure(
            id=d['id'],
            concept_identity=d['conceptIdentity'],
            local_representation=parse_local_representation(d.get('localRepresentation')))

    def parse_dimension_with_pos(d) -> Tuple[int, DataStructureMeta.Dimension]:
        return d['position'], DataStructureMeta.Dimension(
            id=d['id'],
            type=d['type'],
            concept_identity=d['conceptIdentity'],
            local_representation=parse_local_representation(d.get('localRepresentation')))

    def parse_attribute_relations(d) -> Optional[DataStructureMeta.AttributeRelations]:
        if not d:
            return None

        return DataStructureMeta.AttributeRelations(
            primary_measure=d.get('primaryMeasure'),
            dimensions=d.get('dimensions', []),
        )

    def parse_attribute(d) -> DataStructureMeta.Attribute:
        return DataStructureMeta.Attribute(
            id=d['id'],
            assignment_status=d['assignmentStatus'],
            concept_identity=d['conceptIdentity'],
            relationships=parse_attribute_relations(d['attributeRelationship']),
            local_representation=parse_local_representation(d.get('localRepresentation')))

    attributes = []
    dimensions = []

    components = d['dataStructureComponents']

    if 'attributeList' in components:
        attributes = list(map(parse_attribute, components['attributeList']['attributes']))

    if 'dimensionList' in components:
        norm_ds = map(parse_dimension_with_pos, components['dimensionList']['dimensions'])
        time_ds = map(parse_dimension_with_pos, components['dimensionList']['timeDimensions'])
        dimensions = [d for _, d in sorted([*norm_ds, *time_ds], key=lambda x: x[0])]

    primary_measure = parse_measure(components['measureList']['primaryMeasure'])

    return DataStructureMeta(
        id=d['id'],
        structure_url=d.get('structure'),
        name=d['name'],
        version=d['version'],
        primary_measure=primary_measure,
        dimensions={ d.id: d for d in dimensions },
        attributes=attributes,
        raw=d)

