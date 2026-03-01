from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from qdarchive_seeding.app.config_loader import load_config
from qdarchive_seeding.app.container import build_container
from qdarchive_seeding.app.runner import ETLRunner
from qdarchive_seeding.core.entities import RunInfo


class RunService:
    def __init__(self) -> None:
        self._container: Any = None

    async def run(
        self,
        config_path: Path,
        *,
        dry_run: bool = False,
        force: bool = False,
        retry_failed: bool = False,
        on_event: Any | None = None,
    ) -> RunInfo:
        config = load_config(config_path)
        container = build_container(
            config,
            force=force,
            retry_failed=retry_failed,
            enable_log_queue=True,
        )
        self._container = container

        if on_event is not None:
            container.progress_bus.subscribe(on_event)

        runner = ETLRunner(container)
        return await asyncio.to_thread(runner.run, dry_run=dry_run)

    def cancel(self) -> None:
        if self._container is not None:
            pass
