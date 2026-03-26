from __future__ import annotations

import json
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Iterator

from collection_summary import build_collection_summary as _build_collection_summary
from common import (
    CURATED_SCHEMA_VERSION,
    curated_run_key,
    gzip_jsonl,
    indicator_partition_id,
    iter_gzip_jsonl,
    json_dumps,
    load_json_object,
    partition_ids,
    put_object,
    read_gzip_jsonl,
    safe_run_id,
    try_get_object_bytes,
    utc_now_iso,
)
from state_store import load_latest_curated_metadata


def _aggregate_partition_records(records: Iterable[dict[str, Any]], *, generated_at: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    aggregate: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        key = (record["indicator_type"], record["indicator"])
        if key not in aggregate:
            aggregate[key] = {
                "indicator_type": record["indicator_type"],
                "indicator": record["indicator"],
                "record_statuses": set(),
                "source_ids": set(),
                "source_record_ids": set(),
                "brands": set(),
                "last_retrieved_at": record.get("retrieved_at"),
            }
        entry = aggregate[key]
        entry["record_statuses"].add(record.get("record_status") or "unknown")
        if record.get("source_id"):
            entry["source_ids"].add(str(record["source_id"]))
        if record.get("source_record_id"):
            entry["source_record_ids"].add(str(record["source_record_id"]))
        if record.get("target_brand"):
            entry["brands"].add(str(record["target_brand"]))
        current_last = entry["last_retrieved_at"]
        candidate_last = record.get("retrieved_at")
        if candidate_last and (not current_last or candidate_last > current_last):
            entry["last_retrieved_at"] = candidate_last

    combined_records: list[dict[str, Any]] = []
    indicator_counts: defaultdict[str, int] = defaultdict(int)
    for key in sorted(aggregate):
        entry = aggregate[key]
        combined_record = {
            "indicator_type": entry["indicator_type"],
            "indicator": entry["indicator"],
            "record_statuses": sorted(entry["record_statuses"]),
            "source_ids": sorted(entry["source_ids"]),
            "source_record_ids": sorted(entry["source_record_ids"]),
            "brands": sorted(entry["brands"]),
            "last_retrieved_at": entry["last_retrieved_at"],
            "generated_at": generated_at,
        }
        indicator_counts[combined_record["indicator_type"]] += 1
        combined_records.append(combined_record)
    return combined_records, dict(sorted(indicator_counts.items()))


def _aggregate_latest_records(latest_by_source: dict[str, list[dict[str, Any]]], *, generated_at: str) -> tuple[list[dict[str, Any]], dict[str, int]]:
    def _records() -> Iterator[dict[str, Any]]:
        for source_id, records in latest_by_source.items():
            for record in records:
                if record.get("source_id"):
                    yield record
                    continue
                yield {
                    **record,
                    "source_id": source_id,
                }

    return _aggregate_partition_records(_records(), generated_at=generated_at)


def _delta_key(record: dict[str, Any]) -> tuple[str, str]:
    return str(record["indicator_type"]), str(record["indicator"])


def _delta_compare_payload(record: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in record.items()
        if key not in {"generated_at", "last_retrieved_at"}
    }


def _compute_delta_records(
    previous_records: list[dict[str, Any]],
    current_records: list[dict[str, Any]],
    *,
    generated_at: str,
) -> dict[str, list[dict[str, Any]]]:
    previous_by_key = {_delta_key(record): record for record in previous_records}
    current_by_key = {_delta_key(record): record for record in current_records}

    new_records = [current_by_key[key] for key in sorted(set(current_by_key) - set(previous_by_key))]
    removed_records = [
        {
            **previous_by_key[key],
            "delta_kind": "removed",
            "generated_at": generated_at,
        }
        for key in sorted(set(previous_by_key) - set(current_by_key))
    ]
    changed_records: list[dict[str, Any]] = []
    for key in sorted(set(previous_by_key) & set(current_by_key)):
        before = previous_by_key[key]
        after = current_by_key[key]
        if _delta_compare_payload(before) == _delta_compare_payload(after):
            continue
        changed_records.append(
            {
                "indicator_type": after["indicator_type"],
                "indicator": after["indicator"],
                "delta_kind": "changed",
                "generated_at": generated_at,
                "before": before,
                "after": after,
            }
        )
    return {
        "new": new_records,
        "removed": removed_records,
        "changed": changed_records,
    }

def _iter_temp_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def _build_source_summaries(collection_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for result in collection_results:
        summaries.append(
            {
                "source_id": result.get("source_id"),
                "display_name": result.get("display_name"),
                "record_count": result.get("record_count"),
                "normalized_key": result.get("normalized_key"),
                "raw_key": result.get("raw_key"),
                "metadata_url": result.get("metadata_url"),
                "license_url": result.get("license_url"),
                "fetched_at": result.get("fetched_at"),
                "status_code": result.get("status_code"),
                "changed": bool(result.get("changed")),
                "short_circuit_reason": result.get("short_circuit_reason"),
            }
        )
    return sorted(summaries, key=lambda item: str(item.get("source_id") or ""))


def _previous_manifest() -> dict[str, Any] | None:
    latest_metadata = load_latest_curated_metadata(consistent_read=True)
    manifest_key = latest_metadata.get("manifest_key")
    if not manifest_key:
        return None
    return load_json_object(str(manifest_key))


def _previous_partition_records(previous_manifest: dict[str, Any] | None, partition_id: str) -> list[dict[str, Any]]:
    if not previous_manifest:
        return []
    artifacts = previous_manifest.get("artifacts") or {}
    combined_parts = artifacts.get("combined_parts") or {}
    key = combined_parts.get(partition_id)
    if not key:
        return []
    payload = try_get_object_bytes(str(key))
    return read_gzip_jsonl(payload) if payload else []


def _partition_temp_files(collection_results: list[dict[str, Any]], *, part_ids: list[str], run_id: str) -> tuple[str, dict[str, Path]]:
    temp_dir = tempfile.mkdtemp(prefix=f"merge-{safe_run_id(run_id)}-")
    paths = {part_id: Path(temp_dir) / f"records-{part_id}.jsonl" for part_id in part_ids}
    handles = {part_id: paths[part_id].open("w", encoding="utf-8") for part_id in part_ids}
    try:
        for result in collection_results:
            normalized_key = result.get("normalized_key")
            if not normalized_key:
                continue
            for record in iter_gzip_jsonl(str(normalized_key)):
                part_id = indicator_partition_id(record["indicator_type"], record["indicator"])
                handles[part_id].write(json.dumps(record, ensure_ascii=True, sort_keys=True))
                handles[part_id].write("\n")
    finally:
        for handle in handles.values():
            handle.close()
    return temp_dir, paths


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    generated_at = utc_now_iso()
    run_id = str(event.get("run_id", "manual"))
    collection_results = list(event.get("collection_results") or [])
    collection_summary = event.get("collection_summary") or _build_collection_summary(collection_results)
    source_summaries = _build_source_summaries(collection_results)
    previous_manifest = _previous_manifest()
    previous_run_id = previous_manifest.get("run_id") if previous_manifest else None
    part_ids = partition_ids()
    temp_dir, partition_paths = _partition_temp_files(collection_results, part_ids=part_ids, run_id=run_id)

    try:
        indicator_counts: defaultdict[str, int] = defaultdict(int)
        delta_counts = {"new": 0, "changed": 0, "removed": 0}
        combined_record_count = 0
        artifacts = {
            "manifest_key": curated_run_key(run_id, "manifest.json"),
            "partitions": part_ids,
            "combined_prefix": curated_run_key(run_id, "combined/"),
            "combined_parts": {},
            "delta_prefix": curated_run_key(run_id, "deltas/"),
            "delta_parts": {"new": {}, "changed": {}, "removed": {}},
        }

        for part_id in part_ids:
            current_records, part_indicator_counts = _aggregate_partition_records(
                _iter_temp_jsonl(partition_paths[part_id]),
                generated_at=generated_at,
            )
            previous_records = _previous_partition_records(previous_manifest, part_id)
            deltas = _compute_delta_records(previous_records, current_records, generated_at=generated_at)

            combined_key = curated_run_key(run_id, f"combined/part-{part_id}.jsonl.gz")
            put_object(
                combined_key,
                gzip_jsonl(current_records),
                content_type="application/x-ndjson",
                content_encoding="gzip",
                metadata={
                    "generated-at": generated_at,
                    "run-id": run_id,
                    "partition-id": part_id,
                },
            )
            artifacts["combined_parts"][part_id] = combined_key

            for delta_name in ("new", "changed", "removed"):
                delta_key = curated_run_key(run_id, f"deltas/{delta_name}/part-{part_id}.jsonl.gz")
                put_object(
                    delta_key,
                    gzip_jsonl(deltas[delta_name]),
                    content_type="application/x-ndjson",
                    content_encoding="gzip",
                    metadata={
                        "generated-at": generated_at,
                        "run-id": run_id,
                        "partition-id": part_id,
                        "delta-kind": delta_name,
                    },
                )
                artifacts["delta_parts"][delta_name][part_id] = delta_key
                delta_counts[delta_name] += len(deltas[delta_name])

            for indicator_type, count in part_indicator_counts.items():
                indicator_counts[indicator_type] += count
            combined_record_count += len(current_records)

        manifest = {
            "schema_version": CURATED_SCHEMA_VERSION,
            "run_id": run_id,
            "generated_at": generated_at,
            "trigger": event.get("trigger"),
            "requested_by": event.get("requested_by"),
            "run_started_at": event.get("run_started_at"),
            "lookup_sync_mode": event.get("lookup_sync_mode", "delta"),
            "force_merge": bool(event.get("force_merge")),
            "previous_published_run_id": previous_run_id,
            "source_count": len(source_summaries),
            "combined_record_count": combined_record_count,
            "indicator_type_counts": dict(sorted(indicator_counts.items())),
            "delta_counts": delta_counts,
            "collection_summary": collection_summary,
            "sources": source_summaries,
            "artifacts": artifacts,
        }
        put_object(
            artifacts["manifest_key"],
            json_dumps(manifest),
            content_type="application/json",
            metadata={
                "generated-at": generated_at,
                "run-id": run_id,
                "schema-version": str(CURATED_SCHEMA_VERSION),
            },
        )
        return manifest
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)
