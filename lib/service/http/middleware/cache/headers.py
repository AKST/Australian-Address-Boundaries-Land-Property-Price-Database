from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .expiry import CacheExpire

class CacheHeader:
    FORMAT = 'X-Cache-Format'
    EXPIRE = 'X-Cache-Expire'
    DISABLED = 'X-Cache-Disabled'
    LABEL = 'X-Cache-Label'

@dataclass
class InstructionHeaders:
    format: str
    expiry: CacheExpire | None
    disabled: bool
    request_label: Optional[str]

    @property
    def ext(self):
        if self.format == 'json':
            return 'json'
        if self.format == 'text':
            return 'txt'
        raise ValueError(f'unknown format {self.format}')

    @staticmethod
    def from_headers(headers, host):
        headers = headers or {}
        cache_fmt = headers.get(CacheHeader.FORMAT, 'text')
        cache_ttl = headers.get(CacheHeader.EXPIRE, None)
        cache_disabled = headers.get(CacheHeader.DISABLED, 'False') == 'True'
        label = headers.get(CacheHeader.LABEL, None)
        label = f'{host}-{label if label else "?"}'

        if CacheHeader.FORMAT in headers:
            del headers[CacheHeader.FORMAT]
        if CacheHeader.EXPIRE in headers:
            del headers[CacheHeader.EXPIRE]
        if CacheHeader.DISABLED in headers:
            del headers[CacheHeader.DISABLED]
        if CacheHeader.LABEL in headers:
            del headers[CacheHeader.LABEL]

        return headers, InstructionHeaders(
            format=cache_fmt,
            expiry=CacheExpire.parse_expire(cache_ttl),
            disabled=cache_disabled,
            request_label=label,
        )


