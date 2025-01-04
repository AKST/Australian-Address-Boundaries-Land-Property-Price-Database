from typing import List

from .config import Target

def get_static_dirs():
    return [
        '_out_cache',
        '_out_debug',
        '_out_db',
        '_out_state',
        '_out_web',
        '_out_zip',
    ]
