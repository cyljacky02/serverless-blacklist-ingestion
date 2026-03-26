from __future__ import annotations

import json
import logging
import time
from typing import Any

from botocore.exceptions import ClientError

from common import (
    CURATED_LATEST_META_KEY,
    INGESTION_LOCK_KEY,
    INGESTION_LOCK_LEASE_SECONDS,
    state_table,
    utc_epoch_seconds,
    utc_now_iso,
)
from contracts import CuratedLatestPointer, WorkflowLockRecord


logger = logging.getLogger(__name__)


def _log_state_event(payload: dict[str, Any], *, warning: bool = False) -> None:
    level = logging.WARNING if warning else logging.INFO
    logger.log(level, json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str))


def load_source_state(source_id: str, *, consistent_read: bool = False) -> dict[str, Any]:
    response = state_table.get_item(Key={"source_id": source_id}, ConsistentRead=consistent_read)
    return response.get("Item", {})


def save_source_state(source_id: str, attributes: dict[str, Any]) -> None:
    item = {"source_id": source_id}
    item.update(attributes)
    state_table.put_item(Item=item)


def load_latest_curated_metadata(*, consistent_read: bool = True) -> CuratedLatestPointer:
    return load_source_state(CURATED_LATEST_META_KEY, consistent_read=consistent_read)


def save_latest_curated_pointer(pointer: CuratedLatestPointer) -> None:
    save_source_state(
        CURATED_LATEST_META_KEY,
        {
            "record_type": "curated_latest",
            "run_id": pointer.get("run_id"),
            "generated_at": pointer.get("generated_at"),
            "published_at": pointer["published_at"],
            "manifest_key": pointer["manifest_key"],
            "schema_version": pointer.get("schema_version"),
            "combined_record_count": pointer.get("combined_record_count"),
            "delta_counts": pointer.get("delta_counts") or {},
            "source_count": pointer.get("source_count"),
        },
    )


def build_workflow_lock_record(
    event: dict[str, Any],
    *,
    acquired_at: str,
    lease_expires_at: int,
) -> WorkflowLockRecord:
    return {
        "source_id": INGESTION_LOCK_KEY,
        "record_type": "workflow_lock",
        "owner_run_id": str(event.get("run_id", "manual")),
        "trigger": event.get("trigger"),
        "requested_by": event.get("requested_by"),
        "acquired_at": acquired_at,
        "lease_expires_at": lease_expires_at,
        "ttl": lease_expires_at,
    }


def acquire_workflow_lock(event: dict[str, Any], *, attempts: int, sleep_seconds: float) -> dict[str, Any]:
    last_holder: dict[str, Any] = {}
    owner_run_id = str(event.get("run_id", "manual"))

    for attempt in range(1, attempts + 1):
        acquired_at = utc_now_iso()
        now_epoch = utc_epoch_seconds()
        lease_expires_at = now_epoch + INGESTION_LOCK_LEASE_SECONDS
        item = build_workflow_lock_record(event, acquired_at=acquired_at, lease_expires_at=lease_expires_at)

        try:
            state_table.put_item(
                Item=item,
                ConditionExpression=(
                    "attribute_not_exists(source_id) "
                    "OR lease_expires_at < :now_epoch "
                    "OR owner_run_id = :owner_run_id"
                ),
                ExpressionAttributeValues={
                    ":now_epoch": now_epoch,
                    ":owner_run_id": owner_run_id,
                },
            )
            _log_state_event(
                {
                    "component": "workflow_lock",
                    "run_id": owner_run_id,
                    "outcome": "acquired",
                    "attempt_count": attempt,
                    "lease_expires_at": lease_expires_at,
                }
            )
            return {
                **event,
                "lock_acquired": True,
                "lock": {
                    "owner_run_id": item["owner_run_id"],
                    "acquired_at": acquired_at,
                    "lease_expires_at": lease_expires_at,
                    "attempt_count": attempt,
                },
            }
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code != "ConditionalCheckFailedException":
                raise
            last_holder = load_source_state(INGESTION_LOCK_KEY, consistent_read=True)
            if attempt < attempts:
                time.sleep(sleep_seconds)

    _log_state_event(
        {
            "component": "workflow_lock",
            "run_id": owner_run_id,
            "outcome": "skipped",
            "attempt_count": attempts,
            "current_owner_run_id": last_holder.get("owner_run_id"),
            "current_lease_expires_at": last_holder.get("lease_expires_at"),
        },
        warning=True,
    )
    return {
        **event,
        "lock_acquired": False,
        "lock": {
            "attempt_count": attempts,
            "current_owner_run_id": last_holder.get("owner_run_id"),
            "current_lease_expires_at": last_holder.get("lease_expires_at"),
        },
    }


def release_workflow_lock(run_id: str) -> bool:
    released = False
    try:
        state_table.delete_item(
            Key={"source_id": INGESTION_LOCK_KEY},
            ConditionExpression="owner_run_id = :owner_run_id",
            ExpressionAttributeValues={":owner_run_id": run_id},
        )
        released = True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code != "ConditionalCheckFailedException":
            raise

    _log_state_event(
        {
            "component": "workflow_lock",
            "run_id": run_id,
            "outcome": "released" if released else "release_skipped",
        },
        warning=not released,
    )
    return released
