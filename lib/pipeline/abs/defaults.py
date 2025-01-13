from lib.service.static_environment.config import Target

from .config import IngestionSource, FieldTransform as Ft
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
            'SA1_CODE_2021': Ft('sa1_code', 'text'),
            'SA2_CODE_2021': Ft('sa2_code', 'text'),
            'SA3_CODE_2021': Ft('sa3_code', 'text'),
            'SA4_CODE_2021': Ft('sa4_code', 'text'),
            'GCCSA_CODE_2021': Ft('gcc_code', 'text'),
            'STATE_CODE_2021': Ft('state_code', 'text'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'SA2_2021_AUST_GDA2020': {
            'SA2_CODE_2021': Ft('sa2_code', 'text'),
            'SA2_NAME_2021': Ft('sa2_name', 'text'),
            'SA3_CODE_2021': Ft('sa3_code', 'text'),
            'SA4_CODE_2021': Ft('sa4_code', 'text'),
            'GCCSA_CODE_2021': Ft('gcc_code', 'text'),
            'STATE_CODE_2021': Ft('state_code', 'text'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'SA3_2021_AUST_GDA2020': {
            'SA3_CODE_2021': Ft('sa3_code', 'text'),
            'SA3_NAME_2021': Ft('sa3_name', 'text'),
            'SA4_CODE_2021': Ft('sa4_code', 'text'),
            'GCCSA_CODE_2021': Ft('gcc_code', 'text'),
            'STATE_CODE_2021': Ft('state_code', 'text'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'SA4_2021_AUST_GDA2020': {
            'SA4_CODE_2021': Ft('sa4_code', 'text'),
            'SA4_NAME_2021': Ft('sa4_name', 'text'),
            'GCCSA_CODE_2021': Ft('gcc_code', 'text'),
            'STATE_CODE_2021': Ft('state_code', 'text'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'GCCSA_2021_AUST_GDA2020': {
            'GCCSA_CODE_2021': Ft('gcc_code', 'text'),
            'GCCSA_NAME_2021': Ft('gcc_name', 'text'),
            'STATE_CODE_2021': Ft('state_code', 'text'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'STE_2021_AUST_GDA2020': {
            'STATE_CODE_2021': Ft('state_code', 'text'),
            'STATE_NAME_2021': Ft('state_name', 'text'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'MB_2021_AUST_GDA2020': {
            'MB_CODE_2021': Ft('mb_code', 'text'),
            'MB_CATEGORY_2021': Ft('mb_cat', 'text'),
            'SA1_CODE_2021': Ft('sa1_code', 'text'),
            'SA2_CODE_2021': Ft('sa2_code', 'text'),
            'SA3_CODE_2021': Ft('sa3_code', 'text'),
            'SA4_CODE_2021': Ft('sa4_code', 'text'),
            'GCCSA_CODE_2021': Ft('gcc_code', 'text'),
            'STATE_CODE_2021': Ft('state_code', 'text'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
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
            "SAL_CODE_2021": Ft("locality_id", 'text'),
            "SAL_NAME_2021": Ft("locality_name", 'text'),
            "STATE_CODE_2021": Ft("state_code", 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'SED_2021_AUST_GDA2020': {
            "SED_CODE_2021": Ft("electorate_id", 'text'),
            "SED_NAME_2021": Ft("electorate_name", 'text'),
            "STATE_CODE_2021": Ft("state_code", 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'SED_2022_AUST_GDA2020': {
            "SED_CODE_2022": Ft("electorate_id", 'text'),
            "SED_NAME_2022": Ft("electorate_name", 'text'),
            "STATE_CODE_2021": Ft("state_code", 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'SED_2024_AUST_GDA2020': {
            "SED_CODE_2024": Ft("electorate_id", 'text'),
            "SED_NAME_2024": Ft("electorate_name", 'text'),
            "STATE_CODE_2021": Ft("state_code", 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'CED_2021_AUST_GDA2020': {
            "CED_CODE_2021": Ft("electorate_id", 'text'),
            "CED_NAME_2021": Ft("electorate_name", 'text'),
            "STATE_CODE_2021": Ft("state_code", 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'LGA_2021_AUST_GDA2020': {
            "LGA_CODE_2021": Ft("lga_id", 'text'),
            "LGA_NAME_2021": Ft("lga_name", 'text'),
            "STATE_CODE_2021": Ft("state_code", 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'LGA_2022_AUST_GDA2020': {
            "LGA_CODE_2022": Ft("lga_id", 'text'),
            "LGA_NAME_2022": Ft("lga_name", 'text'),
            "STATE_CODE_2021": Ft("state_code", 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'LGA_2023_AUST_GDA2020': {
            "LGA_CODE_2023": Ft("lga_id", 'text'),
            "LGA_NAME_2023": Ft("lga_name", 'text'),
            "STATE_CODE_2021": Ft("state_code", 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'LGA_2024_AUST_GDA2020': {
            "LGA_CODE_2024": Ft("lga_id", 'text'),
            "LGA_NAME_2024": Ft("lga_name", 'text'),
            "STATE_CODE_2021": Ft("state_code", 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'POA_2021_AUST_GDA2020': {
            "POA_CODE_2021": Ft("post_code", 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        # https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/non-abs-structures/destination-zones
        'DZN_2021_AUST_GDA2020': {
            'DZN_CODE_2021': Ft('dzn_code', 'text'),
            'SA2_CODE_2021': Ft('sa2_code', 'text'),
            'STATE_CODE_2021': Ft('state_code', 'text'),
            "AUS_CODE_2021": Ft('in_australia', 'bool'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
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
            'ILOC_CODE_2021': Ft('indigenous_loc_code', 'text'),
            'ILOC_NAME_2021': Ft('indigenous_loc_name', 'text'),
            'IARE_CODE_2021': Ft('indigenous_area_code', 'text'),
            'IREG_CODE_2021': Ft('indigenous_reg_code', 'text'),
            'STATE_CODE_2021': Ft('state_code', 'text'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'IARE_2021_AUST_GDA2020': {
            'IARE_CODE_2021': Ft('indigenous_area_code', 'text'),
            'IARE_NAME_2021': Ft('indigenous_area_name', 'text'),
            'IREG_CODE_2021': Ft('indigenous_reg_code', 'text'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
        'IREG_2021_AUST_GDA2020': {
            'IREG_CODE_2021': Ft('indigenous_reg_code', 'text'),
            'IREG_NAME_2021': Ft('indigenous_reg_name', 'text'),
            'AREA_ALBERS_SQKM': Ft('area_sqkm', 'number'),
            'geometry': Ft('geometry', 'geometry'),
        },
    },
)

