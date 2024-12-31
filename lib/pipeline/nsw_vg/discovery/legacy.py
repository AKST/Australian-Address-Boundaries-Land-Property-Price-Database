from urllib.request import urlopen
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple

from lib.service.http import AbstractClientSession, CacheHeader
from lib.service.static_environment import Target

from .config import NswVgTarget
from .constants import lv_download_page, ps_download_page

ListItem = namedtuple('ListItem', ['Name', 'Link'])

class NswVgBulkSalesScrapeAttempt:
    """
    The NSW Valuer Generals bulk data pages have identical
    layouts, so we can reuse parser this "scrape attempt"
    code between them. There's really only 2 pages, one
    for property sales and one for land values.
    """
    _bulk_data_directory_page: str
    _css_class_path: List[Tuple[str, str]]
    _prefix: str
    _cache_period: str
    _date_fmt: str

    links: List[NswVgTarget]

    def __init__(self,
                 prefix: str,
                 directory_page: str,
                 css_class_path: List[Tuple[str, str]],
                 cache_period: str,
                 date_fmt: str):
        self._prefix = prefix
        self._directory_page = directory_page
        self._css_class_path = css_class_path
        self._cache_period = cache_period
        self._date_fmt = date_fmt

        self.links = []

    async def load_links(self, session: AbstractClientSession) -> List[NswVgTarget]:
        if self.links:
            return self.links
        try:
            async with session.get(self._directory_page, headers={
                CacheHeader.EXPIRE: self._cache_period,
                CacheHeader.FORMAT: 'text',
                CacheHeader.LABEL: 'htmlDirectory',
            }) as response:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')

            parent = soup
            for tag, css_class in self._css_class_path[0:-1]:
                parent = parent.find_all(tag, css_class)[-1]

            self.links = [
                NswVgTarget(
                    url=url,
                    token=None,
                    datetime=datetime.strptime(date_str, self._date_fmt),
                    web_dst=f"{self._prefix}_{date_str.replace(' ', '_')}.zip",
                    zip_dst=f"{self._prefix}_{date_str.replace(' ', '_')}",
                )
                for date_str, url in (
                    (l.get_text().strip(), urljoin(self._directory_page, l['href']))
                    for tag, css_class in self._css_class_path[-1:]
                    for l in parent.find_all(tag, css_class)
                )
            ]
        except Exception as e:
            raise e
        return self.links

    @property
    def latest(self):
        return self.links[-1] if self.links else None

class LandValueDiscovery(NswVgBulkSalesScrapeAttempt):
    def __init__(self):
        super().__init__(prefix='nswvg_lv',
                         directory_page=lv_download_page,
                         css_class_path=[('a', 'btn-lv-data')],
                         cache_period='till_next_day_of_week:Tuesday',
                         date_fmt='%d %b %Y')

class WeeklySalePriceDiscovery(NswVgBulkSalesScrapeAttempt):
    def __init__(self):
        super().__init__(prefix='nswvg_wps',
                         directory_page=ps_download_page,
                         css_class_path=[('div', 'weekly'),
                                         ('a', 'btn-sales-data')],
                         cache_period='till_next_day_of_week:Tuesday',
                         date_fmt='%d %b %Y')

class AnnualSalePriceDiscovery(NswVgBulkSalesScrapeAttempt):
    def __init__(self):
        super().__init__(prefix='nswvg_aps',
                         directory_page=ps_download_page,
                         css_class_path=[('div', 'annual'),
                                         ('a', 'btn-sales-data')],
                         cache_period='delta:weeks:4',
                         date_fmt='%Y')
