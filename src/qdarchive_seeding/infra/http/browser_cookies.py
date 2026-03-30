from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Literal

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BrowserCookieExtractor:
    """Extract cookies from a browser's cookie store for specific domains."""

    browser: Literal["chromium", "chrome", "firefox"] = "chromium"
    _cache: dict[str, str] = field(default_factory=dict, repr=False)

    def get_cookie_header(self, domain: str) -> str:
        """Return a formatted Cookie header value for the given domain.

        Extracts cookies from the configured browser, caches per domain,
        and returns ``name1=val1; name2=val2`` format.
        """
        if domain in self._cache:
            return self._cache[domain]

        cookie_header = self._extract(domain)
        self._cache[domain] = cookie_header
        return cookie_header

    def _extract(self, domain: str) -> str:
        try:
            import browser_cookie3  # type: ignore[import-untyped]
        except ImportError:
            logger.warning("browser_cookie3 not installed — cannot extract browser cookies")
            return ""

        loader = {
            "chromium": browser_cookie3.chromium,
            "chrome": browser_cookie3.chrome,
            "firefox": browser_cookie3.firefox,
        }.get(self.browser)
        if loader is None:
            logger.warning("Unsupported browser: %s", self.browser)
            return ""

        try:
            cookie_jar = loader(domain_name=f".{domain.lstrip('.')}")
            cookies = {c.name: c.value for c in cookie_jar if c.value}
            if not cookies:
                logger.info("No cookies found for %s in %s", domain, self.browser)
                return ""
            header = "; ".join(f"{k}={v}" for k, v in cookies.items())
            logger.debug("Extracted %d cookies for %s", len(cookies), domain)
            return header
        except Exception as exc:
            logger.warning("Failed to extract %s cookies for %s: %s", self.browser, domain, exc)
            return ""
