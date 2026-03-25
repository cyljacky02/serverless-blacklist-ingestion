from __future__ import annotations

from typing import Any

from common import CURATED_LATEST_META_KEY, curated_key, json_dumps, put_object, save_source_state, utc_now_iso


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    published_at = utc_now_iso()
    manifest_key = str(event["artifacts"]["manifest_key"])
    pointer = {
        "schema_version": event.get("schema_version"),
        "run_id": event.get("run_id"),
        "generated_at": event.get("generated_at"),
        "published_at": published_at,
        "manifest_key": manifest_key,
        "combined_record_count": event.get("combined_record_count"),
        "delta_counts": event.get("delta_counts") or {},
        "source_count": event.get("source_count"),
    }

    save_source_state(
        CURATED_LATEST_META_KEY,
        {
            "record_type": "curated_latest",
            "run_id": event.get("run_id"),
            "generated_at": event.get("generated_at"),
            "published_at": published_at,
            "manifest_key": manifest_key,
            "schema_version": event.get("schema_version"),
            "combined_record_count": event.get("combined_record_count"),
            "delta_counts": event.get("delta_counts") or {},
            "source_count": event.get("source_count"),
        },
    )
    put_object(
        curated_key("manifest.json"),
        json_dumps(pointer),
        content_type="application/json",
        metadata={
            "run-id": str(event.get("run_id") or "manual"),
            "generated-at": str(event.get("generated_at") or ""),
            "published-at": published_at,
        },
    )

    return {
        **event,
        "published_latest": pointer,
    }
