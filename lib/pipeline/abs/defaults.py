from lib.service.static_environment.config import Target

from .config import IngestionSource
from .constants import (
    ABS_STRUCTURES_SHAPEFILES,
    NON_ABS_STRUCTURES_SHAPEFILES,
    INDIGENOUS_STRUCTURES_SHAPEFILES,
)

ABS_MAIN_STRUCTURES = IngestionSource(
    gpkg_file='ASGS_2021_MAIN_STRUCTURE_GDA2020.gpkg',
    static_file_target=Target(
        token=None,
        url=ABS_STRUCTURES_SHAPEFILES,
        web_dst='abs_main_structures.zip',
        zip_dst='abs_main_structures',
    ),
    layer_to_table={
        'STE_2021_AUST_GDA2020': 'state',
        'GCCSA_2021_AUST_GDA2020': 'gccsa',
        'SA4_2021_AUST_GDA2020': 'sa4',
        'SA3_2021_AUST_GDA2020': 'sa3',
        'SA2_2021_AUST_GDA2020': 'sa2',
        'SA1_2021_AUST_GDA2020': 'sa1',
        'MB_2021_AUST_GDA2020': 'meshblock'
    },
    database_column_names_for_dataframe_columns={
        'SA1_2021_AUST_GDA2020': {
            'SA1_CODE_2021': 'sa1_code', 'SA2_CODE_2021': 'sa2_code', 'SA3_CODE_2021': 'sa3_code',
            'SA4_CODE_2021': 'sa4_code', 'GCCSA_CODE_2021': 'gcc_code', 'STATE_CODE_2021': 'state_code',
            'AREA_ALBERS_SQKM': 'area_sqkm', 'geometry': 'geometry'
        },
        'SA2_2021_AUST_GDA2020': {
            'SA2_CODE_2021': 'sa2_code', 'SA2_NAME_2021': 'sa2_name', 'SA3_CODE_2021': 'sa3_code',
            'SA4_CODE_2021': 'sa4_code', 'GCCSA_CODE_2021': 'gcc_code', 'STATE_CODE_2021': 'state_code',
            'AREA_ALBERS_SQKM': 'area_sqkm', 'geometry': 'geometry'
        },
        'SA3_2021_AUST_GDA2020': {
            'SA3_CODE_2021': 'sa3_code', 'SA3_NAME_2021': 'sa3_name', 'SA4_CODE_2021': 'sa4_code',
            'GCCSA_CODE_2021': 'gcc_code', 'STATE_CODE_2021': 'state_code', 'AREA_ALBERS_SQKM': 'area_sqkm',
            'geometry': 'geometry'
        },
        'SA4_2021_AUST_GDA2020': {
            'SA4_CODE_2021': 'sa4_code', 'SA4_NAME_2021': 'sa4_name', 'GCCSA_CODE_2021': 'gcc_code',
            'STATE_CODE_2021': 'state_code', 'AREA_ALBERS_SQKM': 'area_sqkm', 'geometry': 'geometry'
        },
        'GCCSA_2021_AUST_GDA2020': {
            'GCCSA_CODE_2021': 'gcc_code', 'GCCSA_NAME_2021': 'gcc_name', 'STATE_CODE_2021': 'state_code',
            'geometry': 'geometry'
        },
        'STE_2021_AUST_GDA2020': {
            'STATE_CODE_2021': 'state_code', 'STATE_NAME_2021': 'state_name', 'geometry': 'geometry'
        },
        'MB_2021_AUST_GDA2020': {
            'MB_CODE_2021': 'mb_code', 'MB_CATEGORY_2021': 'mb_cat',
            'SA1_CODE_2021': 'sa1_code', 'SA2_CODE_2021': 'sa2_code', 'SA3_CODE_2021': 'sa3_code',
            'SA4_CODE_2021': 'sa4_code', 'GCCSA_CODE_2021': 'gcc_code', 'STATE_CODE_2021': 'state_code',
            'AREA_ALBERS_SQKM': 'area_sqkm', 'geometry': 'geometry'
        }
    },
)

NON_ABS_MAIN_STRUCTURES = IngestionSource(
    gpkg_file='ASGS_Ed3_Non_ABS_Structures_GDA2020_updated_2024.gpkg',
    static_file_target=Target(
        token=None,
        url=NON_ABS_STRUCTURES_SHAPEFILES,
        web_dst='non_abs_shape.zip',
        zip_dst='non_abs_structures_shapefiles',
    ),
    layer_to_table={
        'SAL_2021_AUST_GDA2020': 'localities',
        'SED_2021_AUST_GDA2020': 'state_electoral_division_2021',
        'SED_2022_AUST_GDA2020': 'state_electoral_division_2022',
        'SED_2024_AUST_GDA2020': 'state_electoral_division_2024',
        'CED_2021_AUST_GDA2020': 'federal_electoral_division_2021',
        'LGA_2021_AUST_GDA2020': 'lga_2021',
        'LGA_2022_AUST_GDA2020': 'lga_2022',
        'LGA_2023_AUST_GDA2020': 'lga_2023',
        'LGA_2024_AUST_GDA2020': 'lga_2024',
        'POA_2021_AUST_GDA2020': 'post_code',
        'DZN_2021_AUST_GDA2020': 'dzn',
        # Unused
        # - australian drainage divisions, 'ADD_2021_AUST_GDA2020'
        # - tourism regions, 'TR_2021_AUST_GDA2020'
    },
    database_column_names_for_dataframe_columns={
        'SAL_2021_AUST_GDA2020': {
            "SAL_CODE_2021": "locality_id",
            "SAL_NAME_2021": "locality_name",
            "STATE_CODE_2021": "state_code",
            "AUS_CODE_2021": "in_australia",
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry"
        },
        'SED_2021_AUST_GDA2020': {
            "SED_CODE_2021": "electorate_id",
            "SED_NAME_2021": "electorate_name",
            "STATE_CODE_2021": "state_code",
            "AUS_CODE_2021": "in_australia",
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry"
        },
        'SED_2022_AUST_GDA2020': {
            "SED_CODE_2022": "electorate_id",
            "SED_NAME_2022": "electorate_name",
            "STATE_CODE_2021": "state_code",
            "AUS_CODE_2021": "in_australia",
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry"
        },
        'SED_2024_AUST_GDA2020': {
            "SED_CODE_2024": "electorate_id",
            "SED_NAME_2024": "electorate_name",
            "STATE_CODE_2021": "state_code",
            "AUS_CODE_2021": "in_australia",
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry"
        },
        'CED_2021_AUST_GDA2020': {
            "CED_CODE_2021": "electorate_id",
            "CED_NAME_2021": "electorate_name",
            "STATE_CODE_2021": "state_code",
            "AUS_CODE_2021": "in_australia",
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry"
        },
        'LGA_2021_AUST_GDA2020': {
            "LGA_CODE_2021": "lga_id",
            "LGA_NAME_2021": "lga_name",
            "STATE_CODE_2021": "state_code",
            "AUS_CODE_2021": "in_australia",
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry"
        },
        'LGA_2022_AUST_GDA2020': {
            "LGA_CODE_2022": "lga_id",
            "LGA_NAME_2022": "lga_name",
            "STATE_CODE_2021": "state_code",
            "AUS_CODE_2021": "in_australia",
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry"
        },
        'LGA_2023_AUST_GDA2020': {
            "LGA_CODE_2023": "lga_id",
            "LGA_NAME_2023": "lga_name",
            "STATE_CODE_2021": "state_code",
            "AUS_CODE_2021": "in_australia",
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry"
        },
        'LGA_2024_AUST_GDA2020': {
            "LGA_CODE_2024": "lga_id",
            "LGA_NAME_2024": "lga_name",
            "STATE_CODE_2021": "state_code",
            "AUS_CODE_2021": "in_australia",
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry"
        },
        'POA_2021_AUST_GDA2020': {
            "POA_CODE_2021": "post_code",
            "AUS_CODE_2021": "in_australia",
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry"
        },
        # https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/non-abs-structures/destination-zones
        'DZN_2021_AUST_GDA2020': {
            'DZN_CODE_2021': 'dzn_code',
            'SA2_CODE_2021': 'sa2_code',
            'STATE_CODE_2021': 'state_code',
            "AUS_CODE_2021": 'in_australia',
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry",
        },
    },
)

INDIGENOUS_STRUCTURES = IngestionSource(
    gpkg_file='ASGS_Ed3_2021_Indigenous_Structure_GDA2020.gpkg',
    static_file_target=Target(
        token=None,
        url=INDIGENOUS_STRUCTURES_SHAPEFILES,
        web_dst='abs_indigenous_shps.zip',
        zip_dst='abs_indigenous_shps',
    ),
    layer_to_table={
        'ILOC_2021_AUST_GDA2020': 'indigenous_location',
        'IARE_2021_AUST_GDA2020': 'indigenous_area',
        'IREG_2021_AUST_GDA2020': 'indigenous_region',
    },
    database_column_names_for_dataframe_columns={
        'ILOC_2021_AUST_GDA2020': {
            'ILOC_CODE_2021': 'indigenous_loc_code',
            'ILOC_NAME_2021': 'indigenous_loc_name',
            'IARE_CODE_2021': 'indigenous_area_code',
            'IREG_CODE_2021': 'indigenous_reg_code',
            'STATE_CODE_2021': 'state_code',
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry",
        },
        'IARE_2021_AUST_GDA2020': {
            'IARE_CODE_2021': 'indigenous_area_code',
            'IARE_NAME_2021': 'indigenous_area_name',
            'IREG_CODE_2021': 'indigenous_reg_code',
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry",
        },
        'IREG_2021_AUST_GDA2020': {
            'IREG_CODE_2021': 'indigenous_reg_code',
            'IREG_NAME_2021': 'indigenous_reg_name',
            "AREA_ALBERS_SQKM": "area_sqkm",
            "geometry": "geometry",
        },
    },
)

