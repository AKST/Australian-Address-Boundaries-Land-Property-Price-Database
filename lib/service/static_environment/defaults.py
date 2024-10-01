from typing import List

from .config import Target

def get_static_dirs():
    return [
        '_out_web',
        '_out_zip',
        '_out_cache',
        '_out_state',
        '_out_debug',
    ]
