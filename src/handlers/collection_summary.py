from __future__ import annotations

from collections import defaultdict
from typing import Any

from contracts import CollectionSummary, SourceSummary


def build_collection_summary(collection_results: list[dict[str, Any]]) -> CollectionSummary:
    changed_count = 0
    unchanged_count = 0
    status_code_counts: defaultdict[str, int] = defaultdict(int)
    short_circuit_reason_counts: defaultdict[str, int] = defaultdict(int)
    per_source: list[SourceSummary] = []

    for result in collection_results:
        changed = bool(result.get("changed"))
        if changed:
            changed_count += 1
        else:
            unchanged_count += 1
        status_code = result.get("status_code")
        if status_code is not None:
            status_code_counts[str(status_code)] += 1
        reason = str(result.get("short_circuit_reason") or ("changed" if changed else "unknown"))
        short_circuit_reason_counts[reason] += 1
        per_source.append(
            {
                "source_id": result.get("source_id"),
                "changed": changed,
                "short_circuit_reason": reason,
                "status_code": status_code,
                "record_count": result.get("record_count"),
            }
        )

    return {
        "source_count": len(collection_results),
        "changed_count": changed_count,
        "unchanged_count": unchanged_count,
        "status_code_counts": dict(sorted(status_code_counts.items())),
        "short_circuit_reason_counts": dict(sorted(short_circuit_reason_counts.items())),
        "sources": per_source,
    }
