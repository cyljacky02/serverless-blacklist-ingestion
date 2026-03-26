from __future__ import annotations

import json
import logging
from typing import Any, Iterable, Iterator

from common import (
    LOOKUP_METADATA_KEY,
    LOOKUP_SCHEMA_VERSION,
    indicator_lookup_key,
    is_hashed_lookup_key,
    iter_gzip_jsonl,
    lookup_table,
)
from contracts import LookupMetadataRecord, LookupSyncStats


logger = logging.getLogger(__name__)

REBUILD_SCAN_SEGMENTS = 4
REBUILD_SCAN_PROJECTION = "indicator_key, rebuild_run_id"


def _log_lookup_event(payload: dict[str, Any], *, warning: bool = False) -> None:
    level = logging.WARNING if warning else logging.INFO
    logger.log(level, json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str))


def prepare_lookup_item(
    record: dict[str, Any],
    *,
    run_id: str,
    generated_at: str,
    schema_version: int = LOOKUP_SCHEMA_VERSION,
    rebuild_run_id: str | None = None,
) -> dict[str, Any]:
    item = {
        "indicator_key": indicator_lookup_key(record["indicator_type"], record["indicator"]),
        "lookup_identity": f"{record['indicator_type']}#{record['indicator']}",
        "indicator_type": record["indicator_type"],
        "indicator": record["indicator"],
        "record_statuses": list(record.get("record_statuses") or []),
        "source_ids": list(record.get("source_ids") or []),
        "source_record_ids": list(record.get("source_record_ids") or []),
        "brands": list(record.get("brands") or []),
        "last_retrieved_at": record.get("last_retrieved_at"),
        "generated_at": generated_at,
        "run_id": run_id,
        "schema_version": schema_version,
    }
    if rebuild_run_id:
        item["rebuild_run_id"] = rebuild_run_id
    return item


def removed_lookup_keys(records: Iterable[dict[str, Any]]) -> list[str]:
    return [indicator_lookup_key(record["indicator_type"], record["indicator"]) for record in records]


def _artifact_part_keys(artifacts: dict[str, Any], *, kind: str, delta_name: str | None = None) -> list[str]:
    if kind == "combined":
        return [str(key) for _, key in sorted((artifacts.get("combined_parts") or {}).items())]
    if kind == "delta" and delta_name:
        delta_parts = (artifacts.get("delta_parts") or {}).get(delta_name) or {}
        return [str(key) for _, key in sorted(delta_parts.items())]
    return []


def _iter_part_records(keys: Iterable[str]) -> Iterator[dict[str, Any]]:
    for key in keys:
        yield from iter_gzip_jsonl(key)


def load_lookup_metadata(*, consistent_read: bool = False) -> dict[str, Any]:
    if lookup_table is None:
        return {}
    response = lookup_table.get_item(Key={"indicator_key": LOOKUP_METADATA_KEY}, ConsistentRead=consistent_read)
    return response.get("Item") or {}


def _current_rebuild_run_id(*, consistent_read: bool = False) -> str | None:
    return load_lookup_metadata(consistent_read=consistent_read).get("last_rebuild_run_id")


def _write_delta_updates(
    *,
    run_id: str,
    generated_at: str,
    artifacts: dict[str, Any],
    rebuild_run_id: str | None,
) -> tuple[int, int, int, int]:
    upserted_count = 0
    new_count = 0
    changed_count = 0
    deleted_count = 0

    with lookup_table.batch_writer(overwrite_by_pkeys=["indicator_key"]) as batch:
        for record in _iter_part_records(_artifact_part_keys(artifacts, kind="delta", delta_name="new")):
            batch.put_item(
                Item=prepare_lookup_item(
                    record,
                    run_id=run_id,
                    generated_at=generated_at,
                    rebuild_run_id=rebuild_run_id,
                )
            )
            upserted_count += 1
            new_count += 1

        for record in _iter_part_records(_artifact_part_keys(artifacts, kind="delta", delta_name="changed")):
            batch.put_item(
                Item=prepare_lookup_item(
                    record.get("after", record),
                    run_id=run_id,
                    generated_at=generated_at,
                    rebuild_run_id=rebuild_run_id,
                )
            )
            upserted_count += 1
            changed_count += 1

        for record in _iter_part_records(_artifact_part_keys(artifacts, kind="delta", delta_name="removed")):
            batch.delete_item(
                Key={"indicator_key": indicator_lookup_key(record["indicator_type"], record["indicator"])}
            )
            deleted_count += 1

    return upserted_count, new_count, changed_count, deleted_count


def _rebuild_delete_candidate(item: dict[str, Any], *, rebuild_run_id: str) -> bool:
    indicator_key = str(item.get("indicator_key") or "")
    if indicator_key == LOOKUP_METADATA_KEY:
        return False
    if not is_hashed_lookup_key(indicator_key):
        return True
    return item.get("rebuild_run_id") != rebuild_run_id


def _delete_stale_rebuild_items(*, rebuild_run_id: str) -> int:
    deleted_count = 0
    with lookup_table.batch_writer(overwrite_by_pkeys=["indicator_key"]) as batch:
        for segment in range(REBUILD_SCAN_SEGMENTS):
            scan_args: dict[str, Any] = {
                "Segment": segment,
                "TotalSegments": REBUILD_SCAN_SEGMENTS,
                "ConsistentRead": True,
                "ProjectionExpression": REBUILD_SCAN_PROJECTION,
            }
            while True:
                response = lookup_table.scan(**scan_args)
                for item in response.get("Items", []):
                    if not _rebuild_delete_candidate(item, rebuild_run_id=rebuild_run_id):
                        continue
                    batch.delete_item(Key={"indicator_key": item["indicator_key"]})
                    deleted_count += 1
                last_evaluated_key = response.get("LastEvaluatedKey")
                if not last_evaluated_key:
                    break
                scan_args["ExclusiveStartKey"] = last_evaluated_key
    return deleted_count


def _write_rebuild_updates(*, run_id: str, generated_at: str, artifacts: dict[str, Any]) -> tuple[int, int]:
    upserted_count = 0
    with lookup_table.batch_writer(overwrite_by_pkeys=["indicator_key"]) as batch:
        for record in _iter_part_records(_artifact_part_keys(artifacts, kind="combined")):
            batch.put_item(
                Item=prepare_lookup_item(
                    record,
                    run_id=run_id,
                    generated_at=generated_at,
                    rebuild_run_id=run_id,
                )
            )
            upserted_count += 1

    deleted_count = _delete_stale_rebuild_items(rebuild_run_id=run_id)
    return upserted_count, deleted_count


def _save_lookup_metadata(
    *,
    run_id: str,
    generated_at: str,
    lookup_sync_mode: str,
    upserted_count: int,
    new_count: int,
    changed_count: int,
    deleted_count: int,
    source_count: int,
    combined_record_count: int | None,
    delta_counts: dict[str, int],
    collection_summary: dict[str, Any],
    last_rebuild_run_id: str | None,
    manifest_key: str | None,
) -> None:
    metadata_item: LookupMetadataRecord = {
        "indicator_key": LOOKUP_METADATA_KEY,
        "record_type": "metadata",
        "schema_version": LOOKUP_SCHEMA_VERSION,
        "generated_at": generated_at,
        "run_id": run_id,
        "sync_mode": lookup_sync_mode,
        "upserted_count": upserted_count,
        "new_count": new_count,
        "changed_count": changed_count,
        "deleted_count": deleted_count,
        "source_count": source_count,
        "combined_record_count": combined_record_count,
        "delta_counts": delta_counts,
        "collection_summary": collection_summary,
        "last_rebuild_run_id": last_rebuild_run_id,
        "manifest_key": manifest_key,
    }
    lookup_table.put_item(Item=metadata_item)


def sync_lookup_index(
    *,
    run_id: str,
    generated_at: str,
    artifacts: dict[str, Any],
    lookup_sync_mode: str,
    source_count: int,
    combined_record_count: int | None,
    delta_counts: dict[str, int],
    collection_summary: dict[str, Any],
) -> LookupSyncStats:
    if lookup_table is None:
        raise RuntimeError("LOOKUP_TABLE is not configured.")

    last_rebuild_run_id = _current_rebuild_run_id(consistent_read=True)
    upserted_count = 0
    new_count = 0
    changed_count = 0
    deleted_count = 0
    effective_rebuild_run_id = last_rebuild_run_id
    manifest_key = artifacts.get("manifest_key") if isinstance(artifacts, dict) else None

    if lookup_sync_mode == "rebuild":
        upserted_count, deleted_count = _write_rebuild_updates(
            run_id=run_id,
            generated_at=generated_at,
            artifacts=artifacts,
        )
        new_count = upserted_count
        effective_rebuild_run_id = run_id
    else:
        upserted_count, new_count, changed_count, deleted_count = _write_delta_updates(
            run_id=run_id,
            generated_at=generated_at,
            artifacts=artifacts,
            rebuild_run_id=effective_rebuild_run_id,
        )

    _save_lookup_metadata(
        run_id=run_id,
        generated_at=generated_at,
        lookup_sync_mode=lookup_sync_mode,
        upserted_count=upserted_count,
        new_count=new_count,
        changed_count=changed_count,
        deleted_count=deleted_count,
        source_count=source_count,
        combined_record_count=combined_record_count,
        delta_counts=delta_counts,
        collection_summary=collection_summary,
        last_rebuild_run_id=effective_rebuild_run_id,
        manifest_key=manifest_key,
    )

    stats: LookupSyncStats = {
        "run_id": run_id,
        "generated_at": generated_at,
        "lookup_table": lookup_table.name,
        "metadata_key": LOOKUP_METADATA_KEY,
        "sync_mode": lookup_sync_mode,
        "upserted_count": upserted_count,
        "new_count": new_count,
        "changed_count": changed_count,
        "deleted_count": deleted_count,
        "last_rebuild_run_id": effective_rebuild_run_id,
        "manifest_key": manifest_key,
    }
    _log_lookup_event({"component": "lookup_sync", **stats})
    return stats
