LV_WIDE_COLUMNS_MAPPINGS = {
    f"{k} {n}": f"{v}_{n}"
    for n in range(1, 6)
    for k, v in {
        'LAND VALUE': 'land_value',
        'AUTHORITY': 'authority',
        'BASE DATE': 'base_date',
        'BASIS': 'basis',
    }.items()
}

LV_LONG_COLUMN_MAPPINGS = {
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
