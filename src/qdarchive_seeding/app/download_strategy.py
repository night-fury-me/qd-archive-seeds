"""Download-phase dataclasses and ICPSR cookie helpers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from qdarchive_seeding.core.entities import FailureRecord

if TYPE_CHECKING:
    from qdarchive_seeding.app.config_models import PipelineConfig
    from qdarchive_seeding.app.container import Container
    from qdarchive_seeding.app.runner import ConcreteRunContext

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class DownloadDecision:
    """Controls what fraction of collected datasets to download."""

    download_all: bool = True
    percentage: int = 100  # 1-100
    exact_count: int | None = None  # if set, download exactly this many datasets


@dataclass(slots=True)
class DownloadCounters:
    """Mutable download statistics shared across concurrent download tasks."""

    downloaded: int = 0
    failed: int = 0
    skipped: int = 0
    access_denied: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    failures: list[FailureRecord] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class DownloadConfig:
    """Immutable configuration for the download phase."""

    ctx: ConcreteRunContext
    container: Container
    bus: Any  # ProgressBus
    log: logging.Logger
    skip_icpsr: bool
    icpsr_terms_url_callback: Callable[..., Any] | None
    icpsr_prompt_lock: asyncio.Lock
    download_sem: asyncio.Semaphore
    downloads_root: Path
    # Values for progress publishing (read-only)
    extracted: int
    transformed: int
    pre_excluded: int


@dataclass(slots=True)
class DownloadState:
    """Mutable state for the download phase."""

    counters: DownloadCounters
    download_asset_count: int


@dataclass(slots=True)
class DownloadCtx:
    """Bundle of config and state shared by download methods during a pipeline run."""

    cfg: DownloadConfig
    state: DownloadState


def get_icpsr_browser_cookies(
    config: PipelineConfig,
    cache: dict[str, dict[str, str]],
    domain: str = "www.icpsr.umich.edu",
) -> dict[str, str]:
    """Extract and cache browser cookies for an ICPSR domain."""
    if domain in cache:
        return cache[domain]

    ext_auth = config.external_auth.get(domain)
    if ext_auth is None or ext_auth.type != "browser_session":
        cache[domain] = {}
        return cache[domain]

    # Cookie domain for browser lookup (leading dot matches subdomains)
    cookie_domain = "." + domain.removeprefix("www.")

    try:
        import browser_cookie3  # type: ignore[import-untyped]

        loader = {"chromium": browser_cookie3.chromium, "chrome": browser_cookie3.chrome}.get(
            ext_auth.browser, browser_cookie3.chromium
        )
        cookie_jar = loader(domain_name=cookie_domain)
        cache[domain] = {c.name: c.value for c in cookie_jar if c.value}
    except ImportError:
        cache[domain] = {}
        logger.warning(
            "browser_cookie3 not installed — cannot extract browser cookies for %s. "
            "Install browser_cookie3 for ICPSR cookie-based auth: pip install browser_cookie3",
            domain,
        )
    except Exception:
        cache[domain] = {}
        logger.warning("Failed to extract browser cookies for %s", domain)

    return cache[domain]
