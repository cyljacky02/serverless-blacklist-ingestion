from __future__ import annotations

import os
import sys
from pathlib import Path

from botocore.exceptions import ClientError

os.environ.setdefault("ARTIFACTS_BUCKET", "unit-test-bucket")
os.environ.setdefault("STATE_TABLE", "unit-test-table")
os.environ.setdefault("LOOKUP_TABLE", "unit-test-lookup-table")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "handlers"))

import acquire_workflow_lock  # noqa: E402
import release_workflow_lock  # noqa: E402
from common import INGESTION_LOCK_KEY  # noqa: E402


class FakeStateTable:
    def __init__(self, items: dict[str, dict] | None = None) -> None:
        self.items = dict(items or {})

    def put_item(self, *, Item: dict, ConditionExpression: str, ExpressionAttributeValues: dict) -> None:  # noqa: N803
        existing = self.items.get(Item["source_id"])
        now_epoch = ExpressionAttributeValues[":now_epoch"]
        if existing and existing.get("lease_expires_at", 0) >= now_epoch:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem")
        self.items[Item["source_id"]] = Item

    def delete_item(self, *, Key: dict, ConditionExpression: str, ExpressionAttributeValues: dict) -> None:  # noqa: N803
        existing = self.items.get(Key["source_id"])
        if not existing or existing.get("owner_run_id") != ExpressionAttributeValues[":owner_run_id"]:
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException"}}, "DeleteItem")
        self.items.pop(Key["source_id"], None)


def test_acquire_and_release_workflow_lock(monkeypatch) -> None:
    table = FakeStateTable()
    monkeypatch.setattr(acquire_workflow_lock, "state_table", table)
    monkeypatch.setattr(acquire_workflow_lock, "load_source_state", lambda source_id: table.items.get(source_id, {}))
    monkeypatch.setattr(acquire_workflow_lock, "LOCK_ACQUIRE_ATTEMPTS", 1)
    monkeypatch.setattr(release_workflow_lock, "state_table", table)

    acquired = acquire_workflow_lock.handler({"run_id": "run-1", "trigger": "manual", "requested_by": "test"}, None)

    assert acquired["lock_acquired"] is True
    assert table.items[INGESTION_LOCK_KEY]["owner_run_id"] == "run-1"

    released = release_workflow_lock.handler({"run_id": "run-1"}, None)

    assert released["lock_released"] is True
    assert INGESTION_LOCK_KEY not in table.items


def test_acquire_workflow_lock_returns_skip_signal_when_another_run_holds_it(monkeypatch) -> None:
    table = FakeStateTable(
        {
            INGESTION_LOCK_KEY: {
                "source_id": INGESTION_LOCK_KEY,
                "owner_run_id": "run-active",
                "lease_expires_at": 9999999999,
            }
        }
    )
    monkeypatch.setattr(acquire_workflow_lock, "state_table", table)
    monkeypatch.setattr(acquire_workflow_lock, "load_source_state", lambda source_id: table.items.get(source_id, {}))
    monkeypatch.setattr(acquire_workflow_lock, "LOCK_ACQUIRE_ATTEMPTS", 1)

    result = acquire_workflow_lock.handler({"run_id": "run-2", "trigger": "manual", "requested_by": "test"}, None)

    assert result["lock_acquired"] is False
    assert result["lock"]["current_owner_run_id"] == "run-active"
