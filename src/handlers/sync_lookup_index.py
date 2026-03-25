from __future__ import annotations

from typing import Any, Iterable, Iterator

from common import (
    LOOKUP_METADATA_KEY,
    LOOKUP_SCHEMA_VERSION,
    indicator_lookup_key,
    is_hashed_lookup_key,
    iter_gzip_jsonl,
    lookup_table,
)


REBUILD_SCAN_SEGMENTS = 4


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


def _removed_lookup_keys(records: Iterable[dict[str, Any]]) -> list[str]:
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


def _current_rebuild_run_id() -> str | None:
    response = lookup_table.get_item(Key={"indicator_key": LOOKUP_METADATA_KEY})
    item = response.get("Item") or {}
    return item.get("last_rebuild_run_id")


def _write_delta_updates(
    *,
    run_id: str,
    generated_at: str,
    artifacts: dict[str, Any],
    rebuild_run_id: str | None,
) -> tuple[int, int, int, int]:
    new_records = list(_iter_part_records(_artifact_part_keys(artifacts, kind="delta", delta_name="new")))
    changed_records = list(_iter_part_records(_artifact_part_keys(artifacts, kind="delta", delta_name="changed")))
    removed_records = list(_iter_part_records(_artifact_part_keys(artifacts, kind="delta", delta_name="removed")))
    upsert_records = new_records + [record.get("after", record) for record in changed_records]

    with lookup_table.batch_writer(overwrite_by_pkeys=["indicator_key"]) as batch:
        for record in upsert_records:
            batch.put_item(
                Item=prepare_lookup_item(
                    record,
                    run_id=run_id,
                    generated_at=generated_at,
                    rebuild_run_id=rebuild_run_id,
                )
            )
        for lookup_key in _removed_lookup_keys(removed_records):
            batch.delete_item(Key={"indicator_key": lookup_key})

    return len(upsert_records), len(new_records), len(changed_records), len(removed_records)


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
            scan_args: dict[str, Any] = {"Segment": segment, "TotalSegments": REBUILD_SCAN_SEGMENTS}
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


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    if lookup_table is None:
        raise RuntimeError("LOOKUP_TABLE is not configured.")

    run_id = str(event.get("run_id", "manual"))
    generated_at = str(event.get("generated_at"))
    artifacts = event.get("artifacts") or {}
    lookup_sync_mode = str(event.get("lookup_sync_mode") or "delta").strip().lower()
    last_rebuild_run_id = _current_rebuild_run_id()

    upserted_count = 0
    new_count = 0
    changed_count = 0
    deleted_count = 0
    effective_rebuild_run_id = last_rebuild_run_id

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

    with lookup_table.batch_writer(overwrite_by_pkeys=["indicator_key"]) as batch:
        batch.put_item(
            Item={
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
                "source_count": len(event.get("sources") or []),
                "combined_record_count": event.get("combined_record_count"),
                "delta_counts": event.get("delta_counts") or {},
                "collection_summary": event.get("collection_summary") or {},
                "last_rebuild_run_id": effective_rebuild_run_id,
                "manifest_key": (artifacts.get("manifest_key") if isinstance(artifacts, dict) else None),
            }
        )

    return {
        **event,
        "lookup_sync": {
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
        },
    }
