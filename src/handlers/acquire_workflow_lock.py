from __future__ import annotations

import os
import time
from typing import Any

from botocore.exceptions import ClientError

from common import (
    INGESTION_LOCK_KEY,
    INGESTION_LOCK_LEASE_SECONDS,
    load_source_state,
    state_table,
    utc_epoch_seconds,
    utc_now_iso,
)


LOCK_ACQUIRE_ATTEMPTS = int(os.environ.get("LOCK_ACQUIRE_ATTEMPTS", "3"))
LOCK_ACQUIRE_SLEEP_SECONDS = float(os.environ.get("LOCK_ACQUIRE_SLEEP_SECONDS", "2"))


def _lock_item(event: dict[str, Any], *, acquired_at: str, lease_expires_at: int) -> dict[str, Any]:
    return {
        "source_id": INGESTION_LOCK_KEY,
        "record_type": "workflow_lock",
        "owner_run_id": event.get("run_id", "manual"),
        "trigger": event.get("trigger"),
        "requested_by": event.get("requested_by"),
        "acquired_at": acquired_at,
        "lease_expires_at": lease_expires_at,
        "ttl": lease_expires_at,
    }


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    last_holder: dict[str, Any] = {}

    for attempt in range(1, LOCK_ACQUIRE_ATTEMPTS + 1):
        acquired_at = utc_now_iso()
        now_epoch = utc_epoch_seconds()
        lease_expires_at = now_epoch + INGESTION_LOCK_LEASE_SECONDS
        item = _lock_item(event, acquired_at=acquired_at, lease_expires_at=lease_expires_at)

        try:
            state_table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(source_id) OR lease_expires_at < :now_epoch",
                ExpressionAttributeValues={":now_epoch": now_epoch},
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
            last_holder = load_source_state(INGESTION_LOCK_KEY)
            if attempt < LOCK_ACQUIRE_ATTEMPTS:
                time.sleep(LOCK_ACQUIRE_SLEEP_SECONDS)

    return {
        **event,
        "lock_acquired": False,
        "lock": {
            "attempt_count": LOCK_ACQUIRE_ATTEMPTS,
            "current_owner_run_id": last_holder.get("owner_run_id"),
            "current_lease_expires_at": last_holder.get("lease_expires_at"),
        },
    }
