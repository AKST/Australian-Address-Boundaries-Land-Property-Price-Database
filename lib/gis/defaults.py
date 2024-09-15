from .constants import NSW_GOV_ADDR_FEATURE_LAYER, NSW_GOV_LOT_FEATURE_LAYER
from .request import SchemaField, GisSchema, GisProjection, Bounds

SYDNEY_BOUNDS = Bounds(xmin=150.5209, ymin=-34.1183, xmax=151.3430, ymax=-33.5781)
NSW_BOUNDS = Bounds(xmin=140.9990, ymin=-37.5050, xmax=153.6383, ymax=-28.1570)

ADDR_GIS_SCHEMA = GisSchema(
    url=NSW_GOV_ADDR_FEATURE_LAYER,
    id_field='RID',
    result_limit=100,
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
        SchemaField('data', 'housenumber', 2),
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
    result_limit=100,
    fields=[
        SchemaField('id', 'objectid', 1),
        SchemaField('assoc', 'lotidstring', 1),
        SchemaField('assoc', 'controllingauthorityoid', 1),
        SchemaField('assoc', 'cadid', 2),
        
        SchemaField('meta', 'createdate', 1),
        SchemaField('meta', 'modifieddate', 1),
        SchemaField('meta', 'startdate', 1),
        SchemaField('meta', 'enddate', 1),
        SchemaField('meta', 'lastupdate', 1),
        
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
        
        SchemaField('assoc', 'msoid', 3),
        SchemaField('assoc', 'centroidid', 3),
        
        SchemaField('meta', 'shapeuuid', 2),
        SchemaField('meta', 'changetype', 2),
        SchemaField('meta', 'processstate', 3),
        SchemaField('data', 'urbanity', 3),
        SchemaField('geo', 'Shape__Length', 1),
        SchemaField('geo', 'Shape__Area', 1),
    ],
)

WGS84_CRS = 4326

_field_priority = ['id', ('assoc', 2), ('data', 2), ('meta', 2), 'geo']

NSW_ADDRESS_PROJECTION = GisProjection(
    schema=ADDR_GIS_SCHEMA,
    fields=_field_priority,
    epsg_crs=WGS84_CRS)

NSW_LOT_PROJECTION = GisProjection(
    schema=LOT_GIS_SCHEMA,
    fields=_field_priority,
    epsg_crs=WGS84_CRS)