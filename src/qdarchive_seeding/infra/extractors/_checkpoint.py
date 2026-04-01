"""Shared checkpoint/resume utilities for extractors."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qdarchive_seeding.core.interfaces import Checkpoint

logger = logging.getLogger(__name__)


def is_query_done(checkpoint: Checkpoint | None, query_string: str) -> bool:
    """Return True (and log) if the query was already completed in a prior run."""
    if checkpoint is not None and checkpoint.is_query_complete(query_string):
        logger.info("Skipping completed query '%s' (checkpoint)", query_string)
        return True
    return False


def get_resume_page(checkpoint: Checkpoint | None, query_string: str) -> int:
    """Return the page index to resume from (0 if no checkpoint)."""
    if checkpoint is None:
        return 0
    return checkpoint.get_start_page(query_string)


def mark_query_done(checkpoint: Checkpoint | None, query_string: str) -> None:
    """Mark query complete if no failed pages remain, otherwise log a warning."""
    if checkpoint is None:
        return
    if not checkpoint.get_failed_pages(query_string):
        checkpoint.mark_query_complete(query_string)
    else:
        logger.warning(
            "Query '%s' has %d failed pages, not marking complete",
            query_string,
            len(checkpoint.get_failed_pages(query_string)),
        )
