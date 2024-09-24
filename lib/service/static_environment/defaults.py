from .config import Target

import lib.notebook_constants as nb

def get_static_dirs():
    return [
        '_out_web',
        '_out_zip',
        '_out_cache',
        '_out_state',
    ]

def get_static_assets():
    return [
        Target(
            url=nb.non_abs_structures_shapefiles,
            web_dst='non_abs_shape.zip',
            zip_dst='non_abs_structures_shapefiles'),
        # TODO rename this
        Target(url=nb.abs_structures_shapefiles,
               web_dst='cities.zip',
               zip_dst='cities'),
    ]
