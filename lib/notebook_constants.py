
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

gnaf_all_scripts = ['sql/move_gnaf_to_schema.sql']

nsw_adminstrative_boundaries = 'https://data.gov.au/data/dataset/8047ddd1-7193-4667-aef9-b75bc3076075/resource/fc587b12-c699-45a2-aa62-22a5c0e82ef3/download/gda2020.zip'
