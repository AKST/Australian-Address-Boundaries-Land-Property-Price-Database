from dataclasses import dataclass
from typing import Tuple

@dataclass
class Bounds:
    xmin: float
    ymin: float
    ymax: float
    xmax: float

    def x_range(self):
        return self.xmax - self.xmin

    def y_range(self):
        return self.ymax - self.ymin

SYDNEY_BOUNDS = Bounds(xmin=150.5209, ymin=-34.1183, xmax=151.3430, ymax=-33.5781)
NSW_BOUNDS = Bounds(xmin=140.9990, ymin=-37.5050, xmax=153.6383, ymax=-28.1570)

class BoundsIterator:
    def __init__(self, bounds: Bounds, resolution: Tuple[float, float], epsg_crs: int):
        self._bounds = bounds
        self._resolution = resolution
        self.epsg_crs = epsg_crs
        
    def _chunks(self):
        r, _ = self._resolution
        x_min = self._bounds.xmin
        y_min = self._bounds.ymin
        x_range = (self._bounds.xmax - x_min) / r
        y_range = (self._bounds.ymax - y_min) / r

        for xa in range(0, int(x_range), 1):
            for ya in range(0, int(y_range), 1):
                xb, yb = xa + 1, ya + 1
                yield {
                    'xmin': x_min + r * xa,
                    'ymin': y_min + r * ya,
                    'xmax': x_min + r * xb,
                    'ymax': y_min + r * yb,
                    'spatialReference': {'wkid': self.epsg_crs},
                }
        
    def chunks(self):
        ra, rb = self._resolution
        x_min, y_min = self._bounds.xmin, self._bounds.ymin
        x_range = self._bounds.xmax - x_min
        y_range = self._bounds.ymax - y_min

        x_offset = 0.0
        while x_offset < x_range:
            t = x_offset / x_range
            r = ((1.0 - t) * ra) + (t * rb)
            
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