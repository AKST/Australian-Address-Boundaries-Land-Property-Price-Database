from datetime import datetime
from .constants import SPATIAL_NSW_HOST, ENVIRONMENT_NSW_HOST
from .constants import SPATIAL_NSW_LOT_FEATURE_LAYER
from .constants import SPATIAL_NSW_PROP_FEATURE_LAYER
from .constants import ENVIRONMENT_NSW_DA_LAYER
from .constants import ENVIRONMENT_NSW_ZONE_LAYER
from .predicate import DatePredicateFunction, FloatPredicateFunction
from .config import SchemaField, GisSchema, GisProjection, Bounds, FieldPriority

from lib.service.http import HostSemaphoreConfig, BackoffConfig, RetryPreference, HostOverride

_1ST_YEAR = 2000
_NEXT_YEAR = datetime.now().year + 1
_AREA_MAX = 1_000_000_000_000_000

SYDNEY_BOUNDS = Bounds(xmin=150.5209, ymin=-34.1183, xmax=151.3430, ymax=-33.5781)
NSW_BOUNDS = Bounds(xmin=140.9990, ymin=-37.5050, xmax=153.6383, ymax=-28.1570)

GDA2020_CRS = 7844

_field_priority: FieldPriority = ['id', ('assoc', 2), ('data', 2), ('meta', 2), 'geo']

BACKOFF_CONFIG = BackoffConfig(
    RetryPreference(allowed=16),
    hosts={
        SPATIAL_NSW_HOST: HostOverride(pause_other_requests_while_retrying=True),
        ENVIRONMENT_NSW_HOST: HostOverride(pause_other_requests_while_retrying=True),
    },
)

HOST_SEMAPHORE_CONFIG = [
    HostSemaphoreConfig(host=SPATIAL_NSW_HOST, limit=16),
    HostSemaphoreConfig(host=ENVIRONMENT_NSW_HOST, limit=12),
]

SNSW_PROP_SCHEMA = GisSchema(
    url=SPATIAL_NSW_PROP_FEATURE_LAYER,
    db_relation='nsw_spatial_lppt_raw.property_feature_layer',
    debug_field='Shape__Area',
    shard_scheme=[
        DatePredicateFunction.create(field='lastupdate', default_range=(_1ST_YEAR, _NEXT_YEAR)),
        FloatPredicateFunction(field='Shape__Area', default_range=(0.0, _AREA_MAX)),
    ],
    id_field='RID',
    result_limit=100,
    fields=[
        SchemaField('id', 'RID', 1, rename='rid'),
        SchemaField('meta', 'createdate', 1, rename='create_date', format='timestamp_ms'),
        SchemaField('assoc', 'propid', 1, rename='property_id'),
        SchemaField('assoc', 'gurasid', 3),
        SchemaField('assoc', 'principaladdresssiteoid', 2, rename='principal_address_site_oid', format='number'),

        # no real clue what valnet, can only presume they
        # are a company that does valuations or a data
        # base where they are stored.
        SchemaField('data', 'valnetpropertystatus', 3),
        SchemaField('data', 'valnetpropertytype', 3),
        SchemaField('data', 'valnetlotcount', 3),
        SchemaField('assoc', 'valnetworkflowid', 3),

        # No clue what this is
        SchemaField('data', 'propertytype', 2, rename='property_type'),
        SchemaField('data', 'dissolveparcelcount', 3),
        SchemaField('data', 'superlot', 1, rename='super_lot'),
        SchemaField('data', 'housenumber', 3, rename='house_number'),
        SchemaField('data', 'address', 1),
        SchemaField('meta', 'startdate', 1, rename='start_date', format='timestamp_ms'),
        SchemaField('meta', 'enddate', 1, rename='end_date', format='timestamp_ms'),
        SchemaField('meta', 'lastupdate', 1, rename='last_update', format='timestamp_ms'),
        SchemaField('assoc', 'msoid', 3),
        SchemaField('assoc', 'centroidid', 3),
        SchemaField('meta', 'shapeuuid', 2, rename='shape_uuid'),
        SchemaField('meta', 'changetype', 2, rename='change_type'),
        SchemaField('meta', 'processstate', 3),
        SchemaField('data', 'urbanity', 3),
        SchemaField('data', 'principaladdresstype', 2, rename='principle_address_type', format='number'),
        SchemaField('assoc', 'addressstringoid', 1, rename='address_string_oid', format='number'),
        SchemaField('geo', 'Shape__Length', 1, rename='shape_length'),
        SchemaField('geo', 'Shape__Area', 1, rename='shape_area'),
    ],
)

SNSW_PROP_PROJECTION = GisProjection(
    id="nsw_spatial_property",
    schema=SNSW_PROP_SCHEMA,
    fields=_field_priority,
    epsg_crs=GDA2020_CRS)

SNSW_LOT_SCHEMA = GisSchema(
    url=SPATIAL_NSW_LOT_FEATURE_LAYER,
    db_relation='nsw_spatial_lppt_raw.lot_feature_layer',
    debug_field='Shape__Area',
    shard_scheme=[
        DatePredicateFunction.create(field='lastupdate', default_range=(_1ST_YEAR, _NEXT_YEAR)),
        FloatPredicateFunction(field='Shape__Area', default_range=(0.0, _AREA_MAX)),
    ],
    id_field='objectid',
    result_limit=100,
    fields=[
        SchemaField('id', 'objectid', 1, rename='object_id'),
        SchemaField('assoc', 'lotidstring', 1, rename='lot_id_string'),
        SchemaField('assoc', 'controllingauthorityoid', 1, rename='controlling_authority_oid'),
        SchemaField('assoc', 'cadid', 2, rename='cad_id'),

        SchemaField('meta', 'createdate', 1, rename='create_date', format='timestamp_ms'),
        SchemaField('meta', 'modifieddate', 1, rename='modified_date', format='timestamp_ms'),
        SchemaField('meta', 'startdate', 1, rename='start_date', format='timestamp_ms'),
        SchemaField('meta', 'enddate', 1, rename='end_date', format='timestamp_ms'),
        SchemaField('meta', 'lastupdate', 1, rename='last_update', format='timestamp_ms'),

        SchemaField('data', 'planoid', 1, rename='plan_oid', format='number'),
        SchemaField('data', 'plannumber', 1, rename='plan_number', format='number'),
        SchemaField('data', 'planlabel', 1, rename='plan_label'),
        SchemaField('data', 'itstitlestatus', 3),
        SchemaField('data', 'itslotid', 3, rename='its_lot_id'),
        SchemaField('data', 'stratumlevel', 2, rename='stratum_level'),
        SchemaField('data', 'hasstratum', 2, rename='has_stratum'),
        SchemaField('data', 'classsubtype', 3),
        SchemaField('data', 'lotnumber', 1, rename='lot_number'),
        SchemaField('data', 'sectionnumber', 1, rename='section_number'),
        SchemaField('data', 'planlotarea', 1, rename='plan_lot_area', format='number'),
        SchemaField('data', 'planlotareaunits', 1, rename='plan_lot_area_units'),

        SchemaField('assoc', 'msoid', 3),
        SchemaField('assoc', 'centroidid', 3),

        SchemaField('meta', 'shapeuuid', 2, rename='shape_uuid'),
        SchemaField('meta', 'changetype', 2, rename='change_type'),
        SchemaField('meta', 'processstate', 3),
        SchemaField('data', 'urbanity', 3),
        SchemaField('geo', 'Shape__Length', 1, rename='shape_length'),
        SchemaField('geo', 'Shape__Area', 1, rename='shape_area'),
    ],
)

SNSW_LOT_PROJECTION = GisProjection(
    id="nsw_spatial_lot",
    schema=SNSW_LOT_SCHEMA,
    fields=_field_priority,
    epsg_crs=GDA2020_CRS)

ENSW_DA_SCHEMA = GisSchema(
    url=ENVIRONMENT_NSW_DA_LAYER,
    db_relation=None,
    debug_field='STATUS',
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
    id="nsw_planning_da",
    schema=ENSW_DA_SCHEMA,
    fields='*',
    epsg_crs=GDA2020_CRS)


ENSW_ZONE_SCHEMA = GisSchema(
    url=ENVIRONMENT_NSW_ZONE_LAYER,
    db_relation=None,
    debug_field='SYM_CODE',
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
    id="nsw_planning_zoning",
    schema=ENSW_ZONE_SCHEMA,
    fields='*',
    epsg_crs=GDA2020_CRS)

