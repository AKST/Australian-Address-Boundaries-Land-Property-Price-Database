from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import urlencode

from lib.service.http import ClientSession
from .parse import (
    parse_codelist_meta,
    parse_conceptscheme_meta,
    parse_contentconstraints_meta,
    parse_dataflow_meta,
    parse_datastructure_meta,
)
from .types import (
    CategorisationMeta,
    CodeListMeta,
    ConceptSchemeMeta,
    ContentConstraintsMeta,
    DataKey,
    DataFlowMeta,
    DataStructureMeta,
    DimensionAtObservation,
    Frequency,
    StructureType,
    SdmxDataDetail,
    SdmxMetaDetail,
)

_ABS_URL = 'https://data.api.abs.gov.au/rest'
_ACCEPTS = 'application/vnd.sdmx.structure+json'

class AbsClient:
    def __init__(self, session: ClientSession):
        self._session = session


    async def get_data(self,
                       dataflow: DataFlowMeta,
                       data_key: Optional[DataKey] = None,
                       detail: Optional[SdmxDataDetail] = None,
                       observation_dimension: Optional[DimensionAtObservation] = None,
                       start_period: Optional[str] = None,
                       end_period: Optional[str] = None):
        if data_key:
            data_key_s = '.'.join(['+'.join(s) if isinstance(s, list) else (s or '') for s in data_key])
        else:
            data_key_s = 'all'

        params: Dict[str, str] = {}
        if detail:
            params['detail'] = detail
        if start_period:
            params['startPeriod'] = start_period
        if end_period:
            params['endPeriod'] = end_period
        match observation_dimension:
            case DataStructureMeta.Dimension(id=dimension_id):
                params['dimensionAtObservation'] = dimension_id
            case other if isinstance(other, str):
                params['dimensionAtObservation'] = other

        url = f'{_ABS_URL}/data/ABS,{dataflow.id},{dataflow.version}/{data_key_s}'
        if params:
            url = f'{url}?{urlencode(params)}'
        async with self._session.get(url, { 'Accept': 'application/vnd.sdmx.data+json' }) as resp:
            if resp.status == 200:
                return await resp.json()
            print(url, resp.status)
            print(await resp.text())
            raise TypeError()

    async def get_structure_meta(self,
                                 structure_type: StructureType,
                                 structure_id: str,
                                 detail: SdmxMetaDetail,
                                 references: Optional[List[StructureType]] = None):
        params: Dict[str, str] = { 'detail': detail }

        if references:
            params['references'] = ','.join(references)

        url = f'{_ABS_URL}/{structure_type}/ABS/{structure_id}?{urlencode(params)}'
        print(url)
        async with self._session.get(url, { 'Accept': 'application/vnd.sdmx.structure+json' }) as resp:
            if resp.status == 200:
                return await resp.json()
            print(resp.status)
            raise TypeError()

    async def get_dataflow_meta(self) -> List[DataFlowMeta]:
        json = await self.get_structure_meta('dataflow', 'all', 'allcompletestubs')
        dataflows = json['data']['dataflows']
        return list(map(parse_dataflow_meta, dataflows))

    async def find_dataflow_meta(self,
                                 structure_id: str,
                                 references: Optional[List[StructureType]] = None) -> DataFlowMeta:
        json = await self.get_structure_meta('dataflow', structure_id, 'full', references)
        return parse_dataflow_meta(json['data']['dataflows'][0], json['data'])

    async def get_datastructure_meta(self) -> List[DataStructureMeta]:
        json = await self.get_structure_meta('datastructure', 'all', detail='referencestubs')
        datastructures = json['data']['dataStructures']
        return list(map(parse_datastructure_meta, datastructures))

    async def find_datastructure_meta(self, structure_id: str) -> DataStructureMeta:
        json = await self.get_structure_meta('datastructure', structure_id, detail='full')
        datastructures = json['data']['dataStructures']
        return next(map(parse_datastructure_meta, datastructures))

    async def get_codelist_meta(self) -> List[CodeListMeta]:
        json = await self.get_structure_meta('codelist', 'all', detail='full')
        codelists = json['data']['codelists']
        return list(map(parse_codelist_meta, codelists))

    async def find_codelist_meta(self, structure_id: str) -> CodeListMeta:
        json = await self.get_structure_meta('codelist', structure_id, detail='full')
        codelists = json['data']['codelists']
        return next(map(parse_codelist_meta, codelists))

    async def get_categorisation_meta(self) -> List[CategorisationMeta]:
        json = await self.get_structure_meta('categorisation', 'all', detail='full')
        categorisations = json['data']['categorisations']
        return list(map(lambda d: CategorisationMeta(d['id'], d.get('name'), d), categorisations))

    async def find_conceptscheme_meta(self, structure_id: str) -> ConceptSchemeMeta:
        json = await self.get_structure_meta('conceptscheme', structure_id, detail='full')
        conceptschemes = json['data']['conceptSchemes']
        return next(map(parse_conceptscheme_meta, conceptschemes))

    async def find_contentconstraints_meta(self, structure_id: str) -> ContentConstraintsMeta:
        json = await self.get_structure_meta('contentconstraint', structure_id, detail='full')
        contentconstraints = json['data']['contentConstraints']
        return next(map(parse_contentconstraints_meta, contentconstraints))

