from datetime import datetime
from typing import Any, Set, Dict, Tuple, Optional, Iterator
from .types import (
    ConceptSchemeMeta,
    ContentConstraintsMeta,
    DataFlowMeta,
    CodeListMeta,
    DataStructureMeta,
    Observation,
    ObservationMeta,
)

_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

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
    def parse_cuberegion(kv) -> ContentConstraintsMeta.CubeRegion:
        match kv:
            case { 'id': id, 'values': values }:
                return ContentConstraintsMeta.EnumeratedCubeRegion(id, values)

            case { 'id': id, 'timeRange': timeRange }:
                return ContentConstraintsMeta.TimeCubeRegion(
                    id,
                    datetime.strptime(timeRange['startPeriod']['period'], _TIME_FORMAT),
                    timeRange['startPeriod']['isInclusive'],
                    datetime.strptime(timeRange['endPeriod']['period'], _TIME_FORMAT),
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
        ],
        raw=d,
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

    def parse_dimension_with_pos(d) -> DataStructureMeta.Dimension:
        return DataStructureMeta.Dimension(
            id=d['id'],
            type=d['type'],
            position=d['position'],
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


    components = d['dataStructureComponents']

    attributes = []
    if 'attributeList' in components:
        attributes = list(map(parse_attribute, components['attributeList']['attributes']))

    dimensions = {}
    if 'dimensionList' in components:
        norm_ds = map(parse_dimension_with_pos, components['dimensionList']['dimensions'])
        time_ds = map(parse_dimension_with_pos, components['dimensionList']['timeDimensions'])
        dimensions = { d.id: d for d in [*norm_ds, *time_ds] }

    primary_measure = parse_measure(components['measureList']['primaryMeasure'])

    return DataStructureMeta(
        id=d['id'],
        name=d['name'],
        version=d['version'],
        primary_measure=primary_measure,
        dimensions=dimensions,
        dimension_datakey=[d for d in dimensions.values() if d.type == 'Dimension'],
        dimension_time=next((d for d in dimensions.values() if d.type == 'TimeDimension'), None),
        attributes=attributes,
        raw=d)

def parse_data_all_dimensions(resp) -> Iterator[Observation.AllDimensions]:
    def parse_dimension_value(value: Dict[str, Any]) -> ObservationMeta.DimensionValue:
        match value:
            case { 'order': order, 'id': id, 'name': name, **rest }:
                parent = rest.get('parent')
                return ObservationMeta.DimensionEnumableValue(id, name, order, parent)
            case { 'start': start_s, 'end': end_s, 'id': id, 'name': name, **rest }:
                start, end = datetime.strptime(start_s, _TIME_FORMAT), datetime.strptime(end_s, _TIME_FORMAT)
                return ObservationMeta.DimensionTemporalValue(id, name, start, end)
        raise TypeError('unknown data')

    d = resp['data']
    urns = [next(l['urn'] for l in ds['links'] if l['rel'] == 'DataStructure') for ds in d['dataSets']]
    dimension_tables = [
        ObservationMeta.DimensionLookupTable(raw=structure, urn=urns[structure['dataSets'][0]],
            dimensions=[
                ObservationMeta.DimensionDefinition(
                    id=dimension['id'], name=dimension['name'], pos=dimension['keyPosition'],
                    values=[parse_dimension_value(v) for v in dimension['values']] )
                for dimension in structure['dimensions']['observation']])
        for structure in d['structures']
    ]

    for d_idx, dataset in enumerate(d['dataSets']):
        table = dimension_tables[dataset['structure']]
        for values, observations in dataset['observations'].items():
            dimensions = {
                dim.id: ObservationMeta.DimensionRef((d_idx, v_idx), dim.values[v_idx])
                for d_idx, v_idx in enumerate(map(int, values.split(':')))
                for dim in [table.dimensions[d_idx]] }

            for count in observations:
                yield Observation.AllDimensions(urns[d_idx], count, dimensions, table)
