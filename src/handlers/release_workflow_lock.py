from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError

from common import INGESTION_LOCK_KEY, state_table, utc_now_iso


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    run_id = event.get("run_id", "manual")
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

    return {
        **event,
        "lock_released": released,
        "lock_released_at": utc_now_iso(),
    }
