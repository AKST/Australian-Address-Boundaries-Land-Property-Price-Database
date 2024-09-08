from urllib.request import urlopen, urljoin
from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional
import os

from lib.data_types import Target
from lib.gnaf.constants import data_gov_au_gnaf_information_page

_sql_dir_path = 'G-NAF/Extras/GNAF_TableCreation_Scripts'

def _find_subdirectory(directory, subdirectory_name):
    # Walk through the directory tree
    for root, dirs, files in os.walk(directory):
        if subdirectory_name in dirs:
            return os.path.join(root, subdirectory_name)
    return None

@dataclass
class GnafPublicationTarget(Target):
    _psv_dir: Optional[str] = field(default=None)
    
    @property
    def fk_constraints_sql(self):
        return f'./zip-out/{self.zip_dst}/{_sql_dir_path}/add_fk_constraints.sql'
        
    @property
    def create_tables_sql(self):
        return f'./zip-out/{self.zip_dst}/{_sql_dir_path}/create_tables_ansi.sql'

    @property
    def psv_dir(self):
        if self._psv_dir is not None:
            return self._psv_dir
        d = _find_subdirectory(f'./zip-out/{self.zip_dst}', 'Standard')
        if d is None:
            raise Exception('could not find standard dir')
        self._psv_dir = Path(d).parent
        return str(self._psv_dir)
        

class GnafPublicationDiscovery:
    """
    Fetches the latest GNAF publication from the data.gov.au
    information page.
    """
    _attempted: bool
    _publication: Optional[GnafPublicationTarget]

    def __init__(self, information_page_url):
        self._information_page_url = information_page_url
        self._publication = None
        self._attempted = False

    @staticmethod
    def create():
        return GnafPublicationDiscovery(data_gov_au_gnaf_information_page)

    def get_publication(self):
        if self._publication is not None or self._attempted:
            return self._publication
        
        self._attempted = True

        try:
            response = urlopen(self._information_page_url)
            soup = BeautifulSoup(response.read(), 'html.parser')
            link = soup.find(
                'a',
                class_='dropdown-item',
                href=lambda x: x and x.endswith('.zip') and 'gda2020_psv' in x,
            )
            
            if link is not None:
                name = link['href'].split('/')[-1]
                self._publication = GnafPublicationTarget(
                    url=link['href'],
                    web_dst=name,
                    zip_dst=name.split('.')[0],
                )
        finally:
            return self.get_publication()