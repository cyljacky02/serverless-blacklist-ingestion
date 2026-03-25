from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("ARTIFACTS_BUCKET", "unit-test-bucket")
os.environ.setdefault("STATE_TABLE", "unit-test-table")
os.environ.setdefault("LOOKUP_TABLE", "unit-test-lookup-table")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "handlers"))

from reduce_collection_results import handler  # noqa: E402


def test_reduce_collection_results_skips_when_no_change_and_default_delta() -> None:
    result = handler(
        {
            "workflow_options": {},
            "collection_results": [
                {"source_id": "a", "changed": False, "short_circuit_reason": "same_payload_hash", "status_code": 200, "record_count": 1},
                {"source_id": "b", "changed": False, "short_circuit_reason": "not_modified_304", "status_code": 304, "record_count": 2},
            ],
        },
        None,
    )

    assert result["lookup_sync_mode"] == "delta"
    assert result["changed_count"] == 0
    assert result["needs_processing"] is False


def test_reduce_collection_results_rebuild_forces_downstream_work() -> None:
    result = handler(
        {
            "workflow_options": {"lookup_sync_mode": "rebuild"},
            "collection_results": [
                {"source_id": "a", "changed": False, "short_circuit_reason": "same_payload_hash", "status_code": 200, "record_count": 1},
            ],
        },
        None,
    )

    assert result["lookup_sync_mode"] == "rebuild"
    assert result["needs_processing"] is True
