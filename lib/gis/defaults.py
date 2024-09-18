from datetime import datetime
from .constants import SPATIAL_NSW_HOST, ENVIRONMENT_NSW_HOST
from .constants import SPATIAL_NSW_LOT_FEATURE_LAYER
from .constants import SPATIAL_NSW_PROP_FEATURE_LAYER
from .constants import ENVIRONMENT_NSW_DA_LAYER
from .constants import ENVIRONMENT_NSW_ZONE_LAYER
from .predicate import DatePredicateFunction, FloatPredicateFunction
from .request import SchemaField, GisSchema, GisProjection, Bounds
from lib.http.throttled_session import HostSemaphoreConfig

_1ST_YEAR = 2000
_NEXT_YEAR = datetime.now().year + 1
_AREA_MAX = 1_000_000_000_000_000

SYDNEY_BOUNDS = Bounds(xmin=150.5209, ymin=-34.1183, xmax=151.3430, ymax=-33.5781)
NSW_BOUNDS = Bounds(xmin=140.9990, ymin=-37.5050, xmax=153.6383, ymax=-28.1570)

WGS84_CRS = 4326

_field_priority = ['id', ('assoc', 2), ('data', 2), ('meta', 2), 'geo']

HOST_SEMAPHORE_CONFIG = [
    HostSemaphoreConfig(host=SPATIAL_NSW_HOST, limit=1),
    HostSemaphoreConfig(host=ENVIRONMENT_NSW_HOST, limit=12),
]

SNSW_PROP_SCHEMA = GisSchema(
    url=SPATIAL_NSW_PROP_FEATURE_LAYER,
    debug_plot_column='Shape__Area',
    shard_scheme=[
        DatePredicateFunction.create(field='lastupdate', default_range=(_1ST_YEAR, _NEXT_YEAR)),
        FloatPredicateFunction(field='Shape__Area', default_range=(0.0, _AREA_MAX)),
    ],
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

SNSW_PROP_PROJECTION = GisProjection(
    schema=SNSW_PROP_SCHEMA,
    fields=_field_priority,
    epsg_crs=WGS84_CRS)

SNSW_LOT_SCHEMA = GisSchema(
    url=SPATIAL_NSW_LOT_FEATURE_LAYER,
    debug_plot_column='Shape__Area',
    shard_scheme=[
        DatePredicateFunction.create(field='lastupdate', default_range=(_1ST_YEAR, _NEXT_YEAR)),
        FloatPredicateFunction(field='Shape__Area', default_range=(0.0, _AREA_MAX)),
    ],
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

SNSW_LOT_PROJECTION = GisProjection(
    schema=SNSW_LOT_SCHEMA,
    fields=_field_priority,
    epsg_crs=WGS84_CRS)

ENSW_DA_SCHEMA = GisSchema(
    url=ENVIRONMENT_NSW_DA_LAYER,
    debug_plot_column='STATUS',
    shard_scheme=[
        DatePredicateFunction.create(field='SUBMITTED_DATE', default_range=(_1ST_YEAR, _NEXT_YEAR)),
    ],
    id_field='PLANNING_PORTAL_APP_NUMBER',
    result_limit=100,
    fields=[
        SchemaField('id', "PLANNING_PORTAL_APP_NUMBER", 1),
        SchemaField('id', "DA_NUMBER", 1),
        SchemaField('meta', "LAST_UPDATED_DATE", 1),
        SchemaField('meta', "TIMESTAMP", 1),
        SchemaField('meta', "OBJECTID", 1),

        SchemaField('data:timeline', "SUBMITTED_DATE", 1),
        SchemaField('data:timeline', "DETERMINED_DATE", 1),
        SchemaField('data:timeline', "LODGEMENT_DATE", 1),
        SchemaField('data:timeline', "EXHIBIT_OR_NOTIFY_DATE_START", 1),
        SchemaField('data:timeline', "EXHIBIT_OR_NOTIFY_DATE_END", 1),
        SchemaField('data:timeline', "RETURNED_APPLICATION_DATE", 1),
        SchemaField('data:timeline', "SITE_INSPECTION_COMPLETED_DATE", 1),

        SchemaField('data:site', "SITE_ADDRESS", 1),
        SchemaField('data:site', "PRIMARY_ADDRESS", 1),
        SchemaField('data:site', "LGA_NAME", 1),
        SchemaField('data:site', "COUNCIL_NAME", 1),
        SchemaField('data:site', "POSTCODE", 1),
        SchemaField('data:site', "SUBURBNAME", 1),

        SchemaField('data:proposition', "GROSS_FLOOR_AREA_OF_BUILDING", 1),
        SchemaField('data:proposition', "UNITS_OR_DWELLINGS_PROPOSED", 1),
        SchemaField('data:proposition', "STOREYS_PROPOSED", 1),
        SchemaField('data:proposition', "PROPOSED_SUBDIVISION", 1),
        SchemaField('data:proposition', "STAFF_PROPOSED_NUMBER", 1),
        SchemaField('data:proposition', "PARKING_SPACES", 1),
        SchemaField('data:proposition', "LOADING_BAYS", 1),
        SchemaField('data:proposition', "NEW_ROAD_PROPOSED", 1),
        SchemaField('data:proposition', "PROPOSED_HERITAGE_TREE_REMOVAL", 1),
        SchemaField('data:proposition', "PROPOSED_CROWN_DEVELOPMENT", 1),
        SchemaField('data:proposition', "DWELLINGS_TO_BE_DEMOLISHED", 1),
        SchemaField('data:proposition', "DWELLINGS_TO_BE_CONSTRUCTED", 1),

        SchemaField('data', "DEVELOPMENT_SITE_OWNER", 1),
        SchemaField('data', "NUMBER_OF_EXISTING_LOTS", 1),
        SchemaField('data', "PRE_EXISTING_DWELLINGS_ON_SITE", 1),
        SchemaField('data', "PROPOSED_MODIFICATION_DESC", 1),

        SchemaField('data', "ASSESMENT_RESULT", 1),
        SchemaField('data', "DETERMINING_AUTHORITY", 1),
        SchemaField('data', "STATUS", 1),
        SchemaField('data', "APPLICATION_TYPE", 1),
        SchemaField('data', "COST_OF_DEVELOPMENT_RANGE", 1),
        SchemaField('data', "COST_OF_DEVELOPMENT", 1),
        SchemaField('data', "TYPE_OF_DEVELOPMENT", 1),
        SchemaField('data', "APPLIED_ON_BEHALF_OF_COMPANY", 1),
        SchemaField('data', "ANTICIPATED_DETERMINATION_BODY", 1),
        SchemaField('data', "SUBJECT_TO_SIC", 1),
        SchemaField('data', "VPA_NEEDED", 1),
        SchemaField('data', "TYPE_OF_MODIFICATION_REQUESTED", 1),
        SchemaField('data', "DA_NUMBER_PROPOSED_TO_BE_MOD", 1),
        SchemaField('data', "PROPOSED_MODIFICATION_DESC", 1),
        SchemaField('data', "DA_NUMBER_PROPOSED_TO_BE_REVD", 1),
        SchemaField('data', "DEVELOPMENT_DETAILED_DESC", 1),
        SchemaField('data', "STAGED_DEVELOPMENT", 1),
        SchemaField('data', "IS_AN_INTEGRATED_DA", 1),
        SchemaField('data', "IMPACTS_THREATENED_SPECIES", 1),
        SchemaField('data', "DEVELOPMENT_STANDARD_IN_EPI", 1),
        SchemaField('data', "APPROVAL_REQUIRED_UNDER_s68", 1),
        SchemaField('data', "DEV_INC_HERITAGE_ITEM_OR_AREA", 1),
        SchemaField('data', "DEV_PROP_TO_HERITAGE_BUILDINGS", 1),
        SchemaField('data', "NEWBUILD_TO_OTHER_NEWBUILD", 1),
        SchemaField('data', "NEWRESBUILD_TO_ATTACH", 1),
        SchemaField('data', "CONCURRENCE_OR_REFERRAL_SOUGHT", 1),
        SchemaField('data', "APP_NOTIFIED_OR_EXHIBITED", 1),
        SchemaField('geo', "X", 1),
        SchemaField('geo', "Y", 1),
        SchemaField('meta', "TYPE_OF_DEVELOPMENT_GROUPING", 3),
        # SchemaField('geo', "SHAPE", 1),
    ]
)

ENSW_DA_PROJECTION = GisProjection(
    schema=ENSW_DA_SCHEMA,
    fields='*',
    epsg_crs=WGS84_CRS)


ENSW_ZONE_SCHEMA = GisSchema(
    url=ENVIRONMENT_NSW_ZONE_LAYER,
    debug_plot_column='SYM_CODE',
    shard_scheme=[
        DatePredicateFunction.create(field='PUBLISHED_DATE', default_range=(_1ST_YEAR, _NEXT_YEAR)),
    ],
    id_field='OBJECTID',
    result_limit=100,
    fields=[
        SchemaField('data', "OBJECTID", 1),
        SchemaField('data', "EPI_NAME", 2),
        SchemaField('data', "LGA_NAME", 1),
        SchemaField('data', "PUBLISHED_DATE", 1),
        SchemaField('data', "COMMENCED_DATE", 1),
        SchemaField('data', "CURRENCY_DATE", 1),
        SchemaField('data', "AMENDMENT", 1),
        SchemaField('data', "LAY_CLASS", 1),
        SchemaField('data', "SYM_CODE", 1),
        SchemaField('data', "PURPOSE", 1),
        SchemaField('data', "LEGIS_REF_AREA", 1),
        SchemaField('data', "EPI_TYPE", 1),
        # SchemaField('geo', "SHAPE", 1),
    ],
)


ENSW_ZONE_PROJECTION = GisProjection(
    schema=ENSW_ZONE_SCHEMA,
    fields='*',
    epsg_crs=WGS84_CRS)
