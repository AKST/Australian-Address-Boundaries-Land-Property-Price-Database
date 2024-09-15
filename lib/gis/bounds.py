from dataclasses import dataclass, field
from collections import namedtuple
from typing import Tuple

from .request import Bounds

class BoundResolution:
    def interpolate(self, t: float) -> float:
        raise NotImplementedError()

@dataclass
class ExpBoundResolution(BoundResolution):
    start: float
    end: float 
    exp: float = field(default=1.0)
    
    def interpolate(self, t):
        te = t ** self.exp
        return ((1.0 - te) * self.start) + (te * self.end)

@dataclass
class ConstantBoundResolution(BoundResolution):
    value: float
    
    def interpolate(self, t):
        return self.value

class BoundsIterator:
    def __init__(self, 
                 bounds: Bounds,
                 resolution: BoundResolution,
                 epsg_crs: int):
        self._bounds = bounds
        self._resolution = resolution
        self.epsg_crs = epsg_crs
        
    def chunks(self):
        x_min, y_min = self._bounds.xmin, self._bounds.ymin
        x_range = self._bounds.xmax - x_min
        y_range = self._bounds.ymax - y_min

        x_offset = 0.0
        while x_offset < x_range:
            r = self._resolution.interpolate(x_offset / x_range)
            x_frame = r * x_range
            y_frame = r * y_range

            y_offset = 0.0
            while y_offset < y_range:
                yield {
                    'xmin': x_min + x_offset,
                    'ymin': y_min + y_offset,
                    'xmax': x_min + x_offset + x_frame,
                    'ymax': y_min + y_offset + y_frame,
                    'spatialReference': {'wkid': self.epsg_crs},
                }
                y_offset += y_frame
            x_offset += x_frame