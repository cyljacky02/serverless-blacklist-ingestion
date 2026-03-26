from __future__ import annotations

from typing import Any

from common import utc_now_iso
from state_store import release_workflow_lock


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    run_id = event.get("run_id", "manual")
    released = release_workflow_lock(str(run_id))

    return {
        **event,
        "lock_released": released,
        "lock_released_at": utc_now_iso(),
    }
