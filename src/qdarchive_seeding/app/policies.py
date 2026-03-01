from __future__ import annotations

from dataclasses import dataclass

from qdarchive_seeding.core.constants import (
    DOWNLOAD_STATUS_FAILED,
    DOWNLOAD_STATUS_SUCCESS,
    RUN_MODE_INCREMENTAL,
)
from qdarchive_seeding.core.entities import AssetRecord
from qdarchive_seeding.core.interfaces import Policy


@dataclass(slots=True)
class IncrementalPolicy(Policy):
    run_mode: str = RUN_MODE_INCREMENTAL
    force: bool = False

    def should_skip_asset(self, asset: AssetRecord) -> bool:
        if self.force:
            return False
        if (
            self.run_mode == RUN_MODE_INCREMENTAL
            and asset.download_status == DOWNLOAD_STATUS_SUCCESS
        ):
            return True
        return False

    def should_retry(self, error: Exception, attempt: int) -> bool:
        return False


@dataclass(slots=True)
class RetryPolicy(Policy):
    max_attempts: int = 3
    retry_failed: bool = False

    def should_skip_asset(self, asset: AssetRecord) -> bool:
        if asset.download_status == DOWNLOAD_STATUS_SUCCESS:
            return True
        if asset.download_status == DOWNLOAD_STATUS_FAILED and self.retry_failed:
            return False
        if asset.download_status == DOWNLOAD_STATUS_FAILED:
            return True
        return False

    def should_retry(self, error: Exception, attempt: int) -> bool:
        return attempt < self.max_attempts
