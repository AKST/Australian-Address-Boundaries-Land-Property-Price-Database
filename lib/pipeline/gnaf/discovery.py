from urllib.request import urlopen
from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional, Self
import os

from lib.service.http import CacheHeader
from lib.service.static_environment import Target

from .config import PublicationTarget
from .constants import data_gov_au_gnaf_information_page

_sql_dir_path = 'G-NAF/Extras/GNAF_TableCreation_Scripts'

def _find_subdirectory(directory, subdirectory_name):
    # Walk through the directory tree
    for root, dirs, files in os.walk(directory):
        if subdirectory_name in dirs:
            return os.path.join(root, subdirectory_name)
    return None

@dataclass
class GnafPublicationTargetImpl(PublicationTarget):
    _psv_dir: Optional[str] = field(default=None)

    @property
    def fk_constraints_sql(self):
        return f'./_out_zip/{self.zip_dst}/{_sql_dir_path}/add_fk_constraints.sql'

    @property
    def create_tables_sql(self):
        return f'./_out_zip/{self.zip_dst}/{_sql_dir_path}/create_tables_ansi.sql'

    @property
    def psv_dir(self):
        if self._psv_dir is not None:
            return self._psv_dir
        d = _find_subdirectory(f'./_out_zip/{self.zip_dst}', 'Standard')
        if d is None:
            raise Exception('could not find standard dir')
        self._psv_dir = Path(d).parent
        return str(self._psv_dir)


class GnafPublicationDiscovery:
    """
    Fetches the latest GNAF publication from the data.gov.au
    information page.
    """
    publication: Optional[PublicationTarget] = None

    def __init__(self, information_page_url):
        self._information_page_url = information_page_url

    @staticmethod
    def create():
        return GnafPublicationDiscovery(data_gov_au_gnaf_information_page)

    async def load_publication(self: Self, session):
        async with session.get(self._information_page_url, headers={
            CacheHeader.EXPIRE: 'delta:weeks:8',
            CacheHeader.FORMAT: 'text',
            CacheHeader.LABEL: 'gnaf',
        }) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

        link = soup.find(
            'a',
            class_='dropdown-item',
            href=lambda x: x and x.endswith('.zip') and 'gda2020_psv' in x,
        )

        if link is not None:
            name = link['href'].split('/')[-1] # type: ignore
            self.publication = GnafPublicationTargetImpl(
                url=link['href'], # type: ignore
                web_dst=name,
                zip_dst=name.split('.')[0],
                token=None,
            )
