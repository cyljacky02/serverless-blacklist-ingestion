from __future__ import annotations

import os
from typing import Any

from state_store import acquire_workflow_lock


LOCK_ACQUIRE_ATTEMPTS = int(os.environ.get("LOCK_ACQUIRE_ATTEMPTS", "3"))
LOCK_ACQUIRE_SLEEP_SECONDS = float(os.environ.get("LOCK_ACQUIRE_SLEEP_SECONDS", "2"))


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    return acquire_workflow_lock(
        event,
        attempts=LOCK_ACQUIRE_ATTEMPTS,
        sleep_seconds=LOCK_ACQUIRE_SLEEP_SECONDS,
    )
