from typing import List

import lib.notebook_constants as nb

from .config import Target

def get_static_dirs():
    return [
        '_out_web',
        '_out_zip',
        '_out_cache',
        '_out_state',
        '_out_debug',
    ]

def get_static_assets() -> List[Target]:
    return [
        Target(
            token=None,
            url=nb.non_abs_structures_shapefiles,
            web_dst='non_abs_shape.zip',
            zip_dst='non_abs_structures_shapefiles'),
        # TODO rename this
        Target(token=None,
               url=nb.abs_structures_shapefiles,
               web_dst='cities.zip',
               zip_dst='cities'),
    ]
