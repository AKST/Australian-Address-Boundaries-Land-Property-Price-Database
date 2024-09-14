from collections import namedtuple
from dataclasses import dataclass
from typing import List

from .constants import NSW_GOV_ADDR_FEATURE_LAYER, NSW_GOV_LOT_FEATURE_LAYER

SchemaField = namedtuple('SchemaField', ['category', 'name', 'priority'])

@dataclass
class GisSchema:
    url: str
    id_field: str
    fields: List[str]

ADDR_GIS_SCHEMA = GisSchema(
    url=NSW_GOV_ADDR_FEATURE_LAYER,
    id_field='RID',
    fields=[
        SchemaField('id', 'RID', 1),
        SchemaField('meta', 'createdate', 1),
        SchemaField('assoc', 'propid', 1),
        SchemaField('assoc', 'gurasid', 3),
        SchemaField('assoc', 'principaladdresssiteoid', 2),
        
        # no real clue what valnet, can only presume they
        # are a company that does valuations or a data
        # base where they are stored.
        SchemaField('data', 'valnetpropertystatus', 3),
        SchemaField('data', 'valnetpropertytype', 3),
        SchemaField('data', 'valnetlotcount', 3),
        SchemaField('assoc', 'valnetworkflowid', 3),
        
        # No clue what this is
        SchemaField('data', 'propertytype', 2),
        SchemaField('data', 'dissolveparcelcount', 3),
        SchemaField('data', 'superlot', 1),
        SchemaField('data', 'housenumber', 1),
        SchemaField('data', 'address', 1),
        SchemaField('meta', 'startdate', 1),
        SchemaField('meta', 'enddate', 1),
        SchemaField('meta', 'lastupdate', 1),
        SchemaField('assoc', 'msoid', 3),
        SchemaField('assoc', 'centroidid', 3),
        SchemaField('meta', 'shapeuuid', 2),
        SchemaField('meta', 'changetype', 2),
        SchemaField('meta', 'processstate', 3),
        SchemaField('data', 'urbanity', 3),
        SchemaField('data', 'principaladdresstype', 2),
        SchemaField('assoc', 'addressstringoid', 1),
        SchemaField('geo', 'Shape__Length', 1),
        SchemaField('geo', 'Shape__Area', 1),
    ],
)

LOT_GIS_SCHEMA = GisSchema(
    url=NSW_GOV_LOT_FEATURE_LAYER,
    id_field='objectid',
    fields=[
        SchemaField('id', 'objectid', 1),
        SchemaField('assoc', 'cadid', 2),
        SchemaField('meta', 'createdate', 1),
        SchemaField('meta', 'modifieddate', 1),
        SchemaField('assoc', 'controllingauthorityoid', 1),
        SchemaField('data', 'planoid', 1),
        SchemaField('data', 'plannumber', 1),
        SchemaField('data', 'planlabel', 1),
        SchemaField('data', 'itstitlestatus', 3),
        SchemaField('data', 'itslotid', 1),
        SchemaField('data', 'stratumlevel', 2),
        SchemaField('data', 'hasstratum', 2),
        SchemaField('data', 'classsubtype', 3),
        SchemaField('data', 'lotnumber', 1),
        SchemaField('data', 'sectionnumber', 1),
        SchemaField('data', 'planlotarea', 1),
        SchemaField('data', 'planlotareaunits', 1),
        SchemaField('meta', 'startdate', 1),
        SchemaField('meta', 'enddate', 1),
        SchemaField('meta', 'lastupdate', 1),
        SchemaField('assoc', 'msoid', 3),
        SchemaField('assoc', 'centroidid', 3),
        SchemaField('meta', 'shapeuuid', 2),
        SchemaField('meta', 'changetype', 2),
        SchemaField('assoc', 'lotidstring', 1),
        SchemaField('meta', 'processstate', 3),
        SchemaField('data', 'urbanity', 3),
        SchemaField('geo', 'Shape__Length', 1),
        SchemaField('geo', 'Shape__Area', 1),
    ],
)