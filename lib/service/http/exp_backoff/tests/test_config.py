from unittest import TestCase

from lib.service.http.exp_backoff.config import *

class RetryPreferenceTestCase(TestCase):
    def test_apply_override_preserves_unspecified(self):
        over = HostOverride(allowed=3)
        pref = RetryPreference(allowed=4, factor=2, retry_on_client_error=True)
        expect = RetryPreference(allowed=3, factor=2, retry_on_client_error=True)
        self.assertEqual(pref.apply_override(over), expect)

    def test_apply_override_overrides_false(self):
        over = HostOverride(retry_on_client_error=False)
        pref = RetryPreference(allowed=4, factor=2, retry_on_client_error=True)
        expect = RetryPreference(allowed=4, factor=2, retry_on_client_error=False)
        self.assertEqual(pref.apply_override(over), expect)


