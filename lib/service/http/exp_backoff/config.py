from dataclasses import dataclass, field
from typing import Dict


@dataclass
class RetryPreference:
    allowed: int
    factor: int = field(default=2)

    # when ConnectionError gets thrown
    retry_on_connection_error: bool = field(default=True)

    # when you receive a status code >= 400 < 500
    retry_on_client_error: bool = field(default=False)

    # when you receive a status code >= 500 < 600
    retry_on_server_error: bool = field(default=True)

    def backoff_duration(self, attempt):
        return 1.0 * (self.factor ** attempt)

    def can_retry_on_connection_error(self, time):
        return self.retry_on_connection_error and time < self.allowed

    def should_retry(self, status, time):
        if time >= self.allowed:
            return False

        if status >= 500:
            return self.retry_on_server_error
        elif status >= 400:
            return self.retry_on_client_error
        return False

@dataclass
class BackoffConfig:
    default: RetryPreference
    hosts: Dict[str, RetryPreference] = field(default_factory=lambda: {})

    def with_host(self, host):
        return self.hosts[host] if host in self.hosts else self.default
