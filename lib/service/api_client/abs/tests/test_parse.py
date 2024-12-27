import json
import pytest
from pprint import pformat

from lib.service.io import IoService

from ..parse import parse_data_all_dimensions

@pytest.mark.asyncio
@pytest.mark.parametrize("file_name,snap_name", [
    ('abs_data_C21_T17_SA2.._T.AUS...json','data_C21_T17_SA2'),
])
async def test_snapshot(snapshot, file_name: str, snap_name: str):
    io = IoService.create(1)
    resp = json.loads(await io.f_read(f'./_fixtures/{file_name}'))
    data = parse_data_all_dimensions(resp)
    snapshot.assert_match(pformat(list(data), width=150), snap_name)
