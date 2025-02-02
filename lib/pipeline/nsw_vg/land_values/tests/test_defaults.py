from ..defaults import byo_land_values

def test_order_of_defaults():
    assert byo_land_values == sorted(byo_land_values, key=lambda t: t.datetime)

def test_filesnames_correct():
    fnames = [t.src_dst for t in byo_land_values]
    p = '_cfg_byo_lv/nswvg_lv'
    generated = [
        f'{p}_{t.datetime.strftime("%d_%b_%Y")}.zip'
        for t in byo_land_values
    ]
    assert fnames == generated

