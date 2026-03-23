from __future__ import annotations

from qdarchive_seeding.app.policies import IncrementalPolicy, RetryPolicy
from qdarchive_seeding.core.constants import (
    DOWNLOAD_STATUS_FAILED,
    DOWNLOAD_STATUS_RESUMABLE,
    DOWNLOAD_STATUS_SUCCESS,
    RUN_MODE_FULL,
    RUN_MODE_INCREMENTAL,
)
from qdarchive_seeding.core.entities import AssetRecord


def _asset(status: str | None = None) -> AssetRecord:
    return AssetRecord(asset_url="https://example.com/file.pdf", download_status=status)


def test_incremental_skips_success() -> None:
    policy = IncrementalPolicy(run_mode=RUN_MODE_INCREMENTAL, force=False)
    assert policy.should_skip_asset(_asset(DOWNLOAD_STATUS_SUCCESS)) is True


def test_incremental_does_not_skip_failed() -> None:
    policy = IncrementalPolicy(run_mode=RUN_MODE_INCREMENTAL, force=False)
    assert policy.should_skip_asset(_asset(DOWNLOAD_STATUS_FAILED)) is False


def test_incremental_force_does_not_skip() -> None:
    policy = IncrementalPolicy(run_mode=RUN_MODE_INCREMENTAL, force=True)
    assert policy.should_skip_asset(_asset(DOWNLOAD_STATUS_SUCCESS)) is False


def test_full_mode_does_not_skip() -> None:
    policy = IncrementalPolicy(run_mode=RUN_MODE_FULL, force=False)
    assert policy.should_skip_asset(_asset(DOWNLOAD_STATUS_SUCCESS)) is False


def test_incremental_no_retry() -> None:
    policy = IncrementalPolicy()
    assert policy.should_retry(RuntimeError(), 1) is False


def test_retry_policy_skips_success() -> None:
    policy = RetryPolicy(retry_failed=True)
    assert policy.should_skip_asset(_asset(DOWNLOAD_STATUS_SUCCESS)) is True


def test_retry_policy_retries_failed() -> None:
    policy = RetryPolicy(retry_failed=True)
    assert policy.should_skip_asset(_asset(DOWNLOAD_STATUS_FAILED)) is False


def test_retry_policy_skips_failed_when_disabled() -> None:
    policy = RetryPolicy(retry_failed=False)
    assert policy.should_skip_asset(_asset(DOWNLOAD_STATUS_FAILED)) is True


def test_retry_policy_should_retry_under_max() -> None:
    policy = RetryPolicy(max_attempts=3)
    assert policy.should_retry(RuntimeError(), 1) is True
    assert policy.should_retry(RuntimeError(), 2) is True
    assert policy.should_retry(RuntimeError(), 3) is False


def test_incremental_does_not_skip_resumable() -> None:
    policy = IncrementalPolicy(run_mode=RUN_MODE_INCREMENTAL, force=False)
    assert policy.should_skip_asset(_asset(DOWNLOAD_STATUS_RESUMABLE)) is False


def test_retry_policy_does_not_skip_resumable() -> None:
    policy = RetryPolicy(retry_failed=False)
    assert policy.should_skip_asset(_asset(DOWNLOAD_STATUS_RESUMABLE)) is False
