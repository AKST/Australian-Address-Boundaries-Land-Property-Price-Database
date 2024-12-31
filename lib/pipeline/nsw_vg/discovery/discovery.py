from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

from lib.service.http import AbstractClientSession, CacheHeader

from .config import NswVgTarget, NswVgDiscoveryConfig

class NswVgPublicationDiscovery:
    _links: Dict[str, List[NswVgTarget]]

    def __init__(self, session: AbstractClientSession):
        self._session = session
        self._links = {}

    async def load_links(self, cfg: NswVgDiscoveryConfig) -> List[NswVgTarget]:
        if cfg.id in self._links:
            return self._links[cfg.id]

        async with self._session.get(cfg.directory_page, headers={
            CacheHeader.EXPIRE: cfg.cache_period,
            CacheHeader.FORMAT: 'text',
            CacheHeader.LABEL: 'htmlDirectory',
        }) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

        parent = soup
        for tag, css_class in cfg.css_class_path[0:-1]:
            parent = parent.find_all(tag, css_class)[-1]

        self._links[cfg.id] = [
            NswVgTarget(
                url=url,
                token=None,
                datetime=datetime.strptime(date_str, cfg.date_fmt),
                web_dst=f"{cfg.prefix}_{date_str.replace(' ', '_')}.zip",
                zip_dst=f"{cfg.prefix}_{date_str.replace(' ', '_')}",
            )
            for date_str, url in (
                (l.get_text().strip(), urljoin(cfg.directory_page, l['href']))
                for tag, css_class in cfg.css_class_path[-1:]
                for l in parent.find_all(tag, css_class)
            )
        ]
        return self._links[cfg.id]
