from shapely.geometry import box

greater_sydney_max_bounds = box(150, -34, 151.3, -33)

gnaf_image = 'gnaf_pg_gis_db'
gnaf_image_name = 'gnaf_pg_gis_db'
gnaf_image_tag = 'latest'
gnaf_docker_project_label = 'gnaf_pg_gis_db_proj'
gnaf_container = 'gnaf_db'
gnaf_dockerfile_dir = './dockerfiles/postgres';
gnaf_dbname = 'gnaf_db'
gnaf_dbconf = { 'user': 'postgres', 'password': 'throw away password', 'host': 'localhost', 'port': 5432 }
gnaf_db_url = \
    "postgresql+psycopg2://" \
    f"{gnaf_dbconf['user']}:{gnaf_dbconf['password']}@{gnaf_dbconf['host']}:{gnaf_dbconf['port']}/{gnaf_dbname}"

gnaf_dbname_2 = 'gnaf_db_2'
gnaf_dbconf_2 = { **gnaf_dbconf, 'password': 'throw away password 2', 'port': 5433 }
gnaf_db_url_2 = \
    "postgresql+psycopg2://" \
    f"{gnaf_dbconf_2['user']}:{gnaf_dbconf_2['password']}@{gnaf_dbconf_2['host']}:{gnaf_dbconf_2['port']}/{gnaf_dbname_2}"

gnaf_create_tables_script = 'zip-out/gnaf-2020/G-NAF/Extras/GNAF_TableCreation_Scripts/create_tables_ansi.sql'
gnaf_constraints_script = 'zip-out/gnaf-2020/G-NAF/Extras/GNAF_TableCreation_Scripts/add_fk_constraints.sql'
gnaf_all_scripts = ['sql/move_gnaf_to_schema.sql']

# https://data.gov.au/data/dataset/geocoded-national-address-file-g-naf
gnaf_2020 = 'https://data.gov.au/data/dataset/19432f89-dc3a-4ef3-b943-5326ef1dbecc/resource/1c685b96-9297-4b62-888e-c981790d332f/download/g-naf_may24_allstates_gda2020_psv_1015.zip'

nsw_adminstrative_boundaries = 'https://data.gov.au/data/dataset/8047ddd1-7193-4667-aef9-b75bc3076075/resource/fc587b12-c699-45a2-aa62-22a5c0e82ef3/download/gda2020.zip'

non_abs_structures_shapefiles = 'https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/ASGS_Ed3_Non_ABS_Structures_GDA2020_updated_2024.zip'

abs_structures_shapefiles = 'https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/ASGS_2021_MAIN_STRUCTURE_GPKG_GDA2020.zip'

lv_wide_columns_mappings = {
    f"{k} {n}": f"{v}_{n}"
    for n in range(1, 6)
    for k, v in {
        'LAND VALUE': 'land_value',
        'AUTHORITY': 'authority',
        'BASE DATE': 'base_date',
        'BASIS': 'basis',
    }.items()
}
lv_long_column_mappings = {
    'DISTRICT CODE': 'district_code',
    'DISTRICT NAME': 'district_name',
    'PROPERTY ID': 'property_id',
    'PROPERTY TYPE': 'property_type',
    'PROPERTY NAME': 'property_name',
    'UNIT NUMBER': 'unit_number',
    'HOUSE NUMBER': 'house_number',
    'STREET NAME': 'street_name',
    'SUBURB NAME': 'suburb_name',
    'POSTCODE': 'postcode',
    'PROPERTY DESCRIPTION': 'property_description',
    'ZONE CODE': 'zone_code',
    'AREA': 'area',
    'AREA TYPE': 'area_type',
}
lv_raw_dtypes = {
    'DISTRICT CODE': 'int32',
    'DISTRICT NAME': 'string',
    'PROPERTY ID': 'int32',
    'PROPERTY TYPE': 'string',
    'PROPERTY NAME': 'string',
    'UNIT NUMBER': 'string',
    'HOUSE NUMBER': 'string',
    'STREET NAME': 'string',
    'SUBURB NAME': 'string',
    'POSTCODE': 'string',
    'PROPERTY DESCRIPTION': 'string',
    'ZONE CODE': 'string',
    'AREA': 'float32',
    'AREA TYPE': 'string',
    **{
        f"{k} {n}": v
        for n in range(1, 6)
        for k, v in {
            'BASE DATE': 'string',
            'LAND VALUE': 'int32',
            'AUTHORITY': 'string',
            'BASIS': 'string',
        }.items()
    },
    'Unnamed: 34': 'float32'
}

lv_download_page = 'https://www.valuergeneral.nsw.gov.au/land_value_summaries/lv.php'
ps_download_page = 'https://valuation.property.nsw.gov.au/embed/propertySalesInformation'

mb_2016_nsw = 'https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_nsw_shape.zip&1270.0.55.001&Data%20Cubes&E9FA17AFA7EB9FEBCA257FED0013A5F5&0&July%202016&12.07.2016&Latest'
mb_2016_vic = 'https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_vic_shape.zip&1270.0.55.001&Data%20Cubes&04F12B9E465AE765CA257FED0013B20F&0&July%202016&12.07.2016&Latest'
mb_2016_qld = 'https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_qld_shape.zip&1270.0.55.001&Data%20Cubes&A17EA45AB7CC5D5CCA257FED0013B7F6&0&July%202016&12.07.2016&Latest'
mb_2016_sa = 'https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_sa_shape.zip&1270.0.55.001&Data%20Cubes&793662F7A1C04BD6CA257FED0013BCB0&0&July%202016&12.07.2016&Latest'
mb_2016_wa = 'https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_wa_shape.zip&1270.0.55.001&Data%20Cubes&2634B61773C82931CA257FED0013BE47&0&July%202016&12.07.2016&Latest'
mb_2016_tas = 'https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_tas_shape.zip&1270.0.55.001&Data%20Cubes&854152CB547DE707CA257FED0013C180&0&July%202016&12.07.2016&Latest'
mb_2016_nt = 'https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_nt_shape.zip&1270.0.55.001&Data%20Cubes&31364C9DFE4CC667CA257FED0013C4F6&0&July%202016&12.07.2016&Latest'
mb_2016_act = 'https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_act_shape.zip&1270.0.55.001&Data%20Cubes&21B8D5684405A2A7CA257FED0013C567&0&July%202016&12.07.2016&Latest'
mb_2016_other = 'https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055001_mb_2016_ot_shape.zip&1270.0.55.001&Data%20Cubes&9001CEC5D0573AF4CA257FED0013C5F0&0&July%202016&12.07.2016&Latest'

