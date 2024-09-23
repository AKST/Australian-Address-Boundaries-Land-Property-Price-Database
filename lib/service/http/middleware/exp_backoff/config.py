from dataclasses import dataclass, field, asdict
from typing import Dict

@dataclass
class HostOverride:
    allowed: int | None = field(default=None)
    factor: int | None = field(default=None)
    pause_other_requests_while_retrying: bool | None = field(default=None)
    retry_on_connection_error: bool | None = field(default=None)
    retry_on_client_error: bool | None = field(default=None)
    retry_on_server_error: bool | None = field(default=None)

@dataclass
class RetryPreference:
    allowed: int
    factor: int = field(default=2)

    """
    If one request to this host fails, pause others
    to the same host till you succeed.
    """
    pause_other_requests_while_retrying: bool = field(default=False)

    # when ConnectionError gets thrown
    retry_on_connection_error: bool = field(default=True)

    # when you receive a status code >= 400 < 500
    retry_on_client_error: bool = field(default=False)

    # when you receive a status code >= 500 < 600
    retry_on_server_error: bool = field(default=True)

    def backoff_duration(self, attempt: int) -> float:
        return 1.0 * (self.factor ** attempt)

    def can_retry_on_connection_error(self, time: int) -> bool:
        return self.retry_on_connection_error and time < self.allowed

    def should_retry(self, status: int | None, time: int) -> bool:
        if time >= self.allowed:
            return False

        if status is None:
            return self.retry_on_connection_error
        if status >= 500:
            return self.retry_on_server_error
        elif status >= 400:
            return self.retry_on_client_error
        return False

    def apply_override(self, override: HostOverride) -> 'RetryPreference':
        self_d = asdict(self)
        return RetryPreference(**{
            key: value if value is not None else self_d[key]
            for key, value in asdict(override).items()
        })

@dataclass
class BackoffConfig:
    default: RetryPreference
    hosts: Dict[str, HostOverride] = field(default_factory=lambda: {})

    def with_host(self, host: str) -> RetryPreference:
        if host in self.hosts:
            override = self.hosts[host]
            return self.default.apply_override(override)
        else:
            return self.default
