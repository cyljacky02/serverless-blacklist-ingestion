from __future__ import annotations

from typing import Any

from common import indicator_lookup_key
from lookup_store import (
    LOOKUP_METADATA_KEY,
    prepare_lookup_item,
    removed_lookup_keys as _removed_lookup_keys,
    sync_lookup_index,
)

def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    run_id = str(event.get("run_id", "manual"))
    generated_at = str(event.get("generated_at"))
    artifacts = event.get("artifacts") or {}
    lookup_sync_mode = str(event.get("lookup_sync_mode") or "delta").strip().lower()
    lookup_sync = sync_lookup_index(
        run_id=run_id,
        generated_at=generated_at,
        artifacts=artifacts,
        lookup_sync_mode=lookup_sync_mode,
        source_count=len(event.get("sources") or []),
        combined_record_count=event.get("combined_record_count"),
        delta_counts=event.get("delta_counts") or {},
        collection_summary=event.get("collection_summary") or {},
    )

    return {
        **event,
        "lookup_sync": lookup_sync,
    }
