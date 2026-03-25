from __future__ import annotations

from typing import Any

from merge_curated import _build_collection_summary


def _normalize_lookup_sync_mode(value: Any) -> str:
    if str(value or "").strip().lower() == "rebuild":
        return "rebuild"
    return "delta"


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    workflow_options = dict(event.get("workflow_options") or {})
    collection_results = list(event.get("collection_results") or [])
    collection_summary = _build_collection_summary(collection_results)
    changed_count = collection_summary["changed_count"]
    force_merge = bool(workflow_options.get("force_merge") or event.get("force_merge"))
    lookup_sync_mode = _normalize_lookup_sync_mode(workflow_options.get("lookup_sync_mode") or event.get("lookup_sync_mode"))
    needs_processing = changed_count > 0 or force_merge or lookup_sync_mode == "rebuild"

    return {
        **event,
        "force_merge": force_merge,
        "lookup_sync_mode": lookup_sync_mode,
        "changed_count": changed_count,
        "needs_processing": needs_processing,
        "collection_summary": collection_summary,
    }
