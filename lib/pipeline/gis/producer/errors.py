from typing import Any

from lib.pipeline.gis.request import GisProjection

class GisReaderError(Exception):
    offset: Any
    projection: GisProjection

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class MissingResultsError(Exception):
    pass

class GisNetworkError(GisReaderError):
    def __init__(self, http_status, response, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_status = http_status
        self.response = response

class GisTaskNetworkError(GisNetworkError):
    def __init__(self, task, http_status, response, *args, **kwargs):
        super().__init__(http_status, response, *args, **kwargs)
        self.task = task
