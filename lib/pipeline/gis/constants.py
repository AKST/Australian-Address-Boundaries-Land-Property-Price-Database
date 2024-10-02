SPATIAL_NSW_HOST = 'portal.spatial.nsw.gov.au'
SPATIAL_NSW = \
    f'https://{SPATIAL_NSW_HOST}' \
    '/server/rest/services/NSW_Land_Parcel_Property_Theme/FeatureServer'

SPATIAL_NSW_PROP_FEATURE_LAYER = f'{SPATIAL_NSW}/12'
SPATIAL_NSW_LOT_FEATURE_LAYER = f'{SPATIAL_NSW}/8'

'https://mapprod3.environment.nsw.gov.au/arcgis/rest/services/Planning/Principal_Planning_Layers/MapServer/11?f=pjson'
ENVIRONMENT_NSW_HOST = 'mapprod3.environment.nsw.gov.au'
ENVIRONMENT_NSW = \
    f'https://{ENVIRONMENT_NSW_HOST}' \
    '/arcgis/rest/services/Planning'

ENVIRONMENT_NSW_PPL = f'{ENVIRONMENT_NSW}/Principal_Planning_Layers/MapServer'
ENVIRONMENT_NSW_DA = f'{ENVIRONMENT_NSW}/DA_Tracking/MapServer'

ENVIRONMENT_NSW_DA_LAYER = f'{ENVIRONMENT_NSW_DA}/0'
ENVIRONMENT_NSW_ZONE_LAYER = f'{ENVIRONMENT_NSW_PPL}/11'
ENVIRONMENT_NSW_HERITAGE_LAYER =  f'{ENVIRONMENT_NSW_PPL}/8'