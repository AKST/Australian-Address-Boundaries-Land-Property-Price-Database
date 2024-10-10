import pytest

from ..rows import read_int, read_optional_int, read_zone_std

@pytest.mark.parametrize("code,std", [
    ('A', 'legacy_vg_2011'),
    ('AGB', 'unknown'),
    ('B', 'legacy_vg_2011'),
    ('B1', 'ep&a_2006'),
    ('B2', 'ep&a_2006'),
    ('B3', 'ep&a_2006'),
    ('B4', 'ep&a_2006'),
    ('B5', 'ep&a_2006'),
    ('B6', 'ep&a_2006'),
    ('B7', 'ep&a_2006'),
    ('B8', 'ep&a_2006'),
    ('C', 'legacy_vg_2011'),
    ('C1', 'ep&a_2006'),
    ('C2', 'ep&a_2006'),
    ('C3', 'ep&a_2006'),
    ('C4', 'ep&a_2006'),
    ('D', 'legacy_vg_2011'),
    ('E', 'legacy_vg_2011'),
    ('E1', 'ep&a_2006'),
    ('E2', 'ep&a_2006'),
    ('E3', 'ep&a_2006'),
    ('E4', 'ep&a_2006'),
    ('E5', 'ep&a_2006'),
    ('EM', 'unknown'),
    ('ENT', 'unknown'),
    ('ENZ', 'unknown'),
    ('I', 'legacy_vg_2011'),
    ('IN1', 'ep&a_2006'),
    ('IN2', 'ep&a_2006'),
    ('IN3', 'ep&a_2006'),
    ('IN4', 'ep&a_2006'),
    ('M', 'legacy_vg_2011'),
    ('MU', 'unknown'),
    ('MU1', 'ep&a_2006'),
    ('N', 'legacy_vg_2011'),
    ('O', 'legacy_vg_2011'),
    ('P', 'legacy_vg_2011'),
    ('R', 'legacy_vg_2011'),
    ('R1', 'ep&a_2006'),
    ('R2', 'ep&a_2006'),
    ('R3', 'ep&a_2006'),
    ('R4', 'ep&a_2006'),
    ('R5', 'ep&a_2006'),
    ('RE1', 'ep&a_2006'),
    ('RE2', 'ep&a_2006'),
    ('REZ', 'unknown'),
    ('RU1', 'ep&a_2006'),
    ('RU2', 'ep&a_2006'),
    ('RU3', 'ep&a_2006'),
    ('RU4', 'ep&a_2006'),
    ('RU5', 'ep&a_2006'),
    ('RU6', 'ep&a_2006'),
    ('S', 'legacy_vg_2011'),
    ('SP1', 'ep&a_2006'),
    ('SP2', 'ep&a_2006'),
    ('SP3', 'ep&a_2006'),
    ('SP4', 'ep&a_2006'),
    ('SP5', 'ep&a_2006'),
    ('T', 'legacy_vg_2011'),
    ('U', 'legacy_vg_2011'),
    ('UD', 'unknown'),
    ('UR', 'unknown'),
    ('V', 'legacy_vg_2011'),
    ('W', 'legacy_vg_2011'),
    ('W1', 'ep&a_2006'),
    ('W2', 'ep&a_2006'),
    ('W3', 'ep&a_2006'),
    ('X', 'legacy_vg_2011'),
    ('Y', 'legacy_vg_2011'),
    ('Z', 'legacy_vg_2011'),
])
def test_zone_code_check(code, std):
    assert read_zone_std([code], 0, 'zone') == std

@pytest.mark.parametrize("row, index, result", [
    (["5", "asdf", "asdfaf"], 0, 5),
    (["sdfas", "23", "afaf"], 1, 23),
])
def test_read_int(row, index, result):
    assert read_int(row, index, 'blah') == result

@pytest.mark.parametrize("row, index, result", [
    (["5", "asdf", "asdfaf"], 0, 5),
    (["", "asdf", "asdfaf"], 0, None),
    (["sdfas", "23", "afaf"], 1, 23),
    (["sdfas", "", "afaf"], 1, None),
])
def test_read_optional_int(row, index, result):
    assert read_optional_int(row, index, 'blah') == result
