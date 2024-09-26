import pytest

from ..factories import read_int, read_optional_int


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
