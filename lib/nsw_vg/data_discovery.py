from urllib.request import urlopen, urljoin
from bs4 import BeautifulSoup
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Tuple

from lib.data_types import Target
from lib.notebook_constants import lv_download_page, ps_download_page

ListItem = namedtuple('ListItem', ['Name', 'Link'])

@dataclass
class NswVgTarget(Target):
    datetime: datetime = field(default=None)
    

class NswValuerGeneralBulkSalesScrapeAttempt:
    """
    The NSW Valuer Generals bulk data pages have identical
    layouts, so we can reuse parser this "scrape attempt"
    code between them. There's really only 2 pages, one
    for property sales and one for land values.
    """
    _bulk_data_directory_page: str
    _css_class_path: List[Tuple[str, str]]
    _attempted = False
    _prefix: str
    _links: Optional[List[Target]] = None
    _date_fmt: str
    
    def __init__(self,
                 prefix: str,
                 directory_page: str,
                 css_class_path: List[Tuple[str, str]],
                 date_fmt: str):
        self._prefix = prefix
        self._directory_page = directory_page
        self._css_class_path = css_class_path
        self._date_fmt = date_fmt

    def get_links(self):
        if self._links is not None or self._attempted:
            return self._links

        self._attempted = True

        try:
            response = urlopen(self._directory_page)
            soup = BeautifulSoup(response.read(), 'html.parser')

            parent = soup
            for tag, css_class in self._css_class_path[0:-1]:
                parent = parent.find_all(tag, css_class)[-1]

            self._links = [
                NswVgTarget(
                    url=url,
                    datetime=datetime.strptime(date, self._date_fmt),
                    web_dst=f"{self._prefix}_{date.replace(' ', '_')}.zip",
                    zip_dst=f"{self._prefix}_{date.replace(' ', '_')}",
                )
                for date, url in (
                    (l.get_text().strip(), urljoin(self._directory_page, l['href']))
                    for tag, css_class in self._css_class_path[-1:]
                    for l in parent.find_all(tag, css_class)
                )
            ]
        finally:
            pass
        
        return self.get_links()

    def get_latest(self):
        return self.get_links()[-1]

class LandValueDiscovery(NswValuerGeneralBulkSalesScrapeAttempt):
    def __init__(self):
        super().__init__(prefix='nswvg_lv',
                         directory_page=lv_download_page,
                         css_class_path=[('a', 'btn-lv-data')],
                         date_fmt='%d %b %Y')

class WeeklySalePriceDiscovery(NswValuerGeneralBulkSalesScrapeAttempt):
    def __init__(self):
        super().__init__(prefix='nswvg_wps',
                         directory_page=ps_download_page,
                         css_class_path=[('div', 'weekly'),
                                         ('a', 'btn-sales-data')],
                         date_fmt='%d %b %Y')

class AnnualSalePriceDiscovery(NswValuerGeneralBulkSalesScrapeAttempt):
    def __init__(self):
        super().__init__(prefix='nswvg_aps',
                         directory_page=ps_download_page,
                         css_class_path=[('div', 'annual'),
                                         ('a', 'btn-sales-data')],
                         date_fmt='%Y')