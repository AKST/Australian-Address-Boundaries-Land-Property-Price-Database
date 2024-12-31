from .config import NswVgDiscoveryConfig
from .constants import lv_download_page

NSWVG_LV_DISCOVERY_CFG = NswVgDiscoveryConfig(
    id='NSWVG_LV_DISCOVERY_CFG',
    prefix='nswvg_lv',
    directory_page=lv_download_page,
    css_class_path=[('a', 'btn-lv-data')],
    cache_period='till_next_day_of_week:Tuesday',
    date_fmt='%d %b %Y',
)
