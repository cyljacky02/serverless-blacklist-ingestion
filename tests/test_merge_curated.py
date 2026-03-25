from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("ARTIFACTS_BUCKET", "unit-test-bucket")
os.environ.setdefault("STATE_TABLE", "unit-test-table")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "handlers"))

from merge_curated import _aggregate_latest_records, _build_collection_summary, _compute_delta_records  # noqa: E402


def test_aggregate_latest_records_merges_sources() -> None:
    latest_by_source = {
        "source_a": [
            {
                "indicator_type": "url",
                "indicator": "https://a.example/",
                "record_status": "active",
                "source_record_id": "a1",
                "target_brand": "Alpha",
                "retrieved_at": "2026-03-26T00:00:00Z",
            }
        ],
        "source_b": [
            {
                "indicator_type": "url",
                "indicator": "https://a.example/",
                "record_status": "active",
                "source_record_id": "b1",
                "target_brand": "Alpha",
                "retrieved_at": "2026-03-27T00:00:00Z",
            }
        ],
    }

    combined, counts = _aggregate_latest_records(latest_by_source, generated_at="2026-03-27T00:00:00Z")

    assert counts == {"url": 1}
    assert combined[0]["indicator"] == "https://a.example/"
    assert combined[0]["source_ids"] == ["source_a", "source_b"]
    assert combined[0]["last_retrieved_at"] == "2026-03-27T00:00:00Z"


def test_compute_delta_records_splits_new_removed_and_changed() -> None:
    previous = [
        {
            "indicator_type": "url",
            "indicator": "https://a.example/",
            "record_statuses": ["active"],
            "source_ids": ["source_a"],
            "source_record_ids": ["a1"],
            "brands": ["Alpha"],
            "last_retrieved_at": "2026-03-26T00:00:00Z",
            "generated_at": "2026-03-26T00:00:00Z",
        },
        {
            "indicator_type": "url",
            "indicator": "https://gone.example/",
            "record_statuses": ["active"],
            "source_ids": ["source_a"],
            "source_record_ids": ["gone1"],
            "brands": [],
            "last_retrieved_at": "2026-03-26T00:00:00Z",
            "generated_at": "2026-03-26T00:00:00Z",
        },
    ]
    current = [
        {
            "indicator_type": "url",
            "indicator": "https://a.example/",
            "record_statuses": ["blocked"],
            "source_ids": ["source_a"],
            "source_record_ids": ["a1"],
            "brands": ["Alpha"],
            "last_retrieved_at": "2026-03-27T00:00:00Z",
            "generated_at": "2026-03-27T00:00:00Z",
        },
        {
            "indicator_type": "url",
            "indicator": "https://new.example/",
            "record_statuses": ["active"],
            "source_ids": ["source_b"],
            "source_record_ids": ["new1"],
            "brands": ["New"],
            "last_retrieved_at": "2026-03-27T00:00:00Z",
            "generated_at": "2026-03-27T00:00:00Z",
        },
    ]

    deltas = _compute_delta_records(previous, current, generated_at="2026-03-27T00:00:00Z")

    assert [record["indicator"] for record in deltas["new"]] == ["https://new.example/"]
    assert [record["indicator"] for record in deltas["removed"]] == ["https://gone.example/"]
    assert [record["indicator"] for record in deltas["changed"]] == ["https://a.example/"]


def test_build_collection_summary_counts_reasons_and_statuses() -> None:
    summary = _build_collection_summary(
        [
            {"source_id": "a", "changed": False, "short_circuit_reason": "not_modified_304", "status_code": 304, "record_count": 1},
            {"source_id": "b", "changed": False, "short_circuit_reason": "same_payload_hash", "status_code": 200, "record_count": 2},
            {"source_id": "c", "changed": True, "short_circuit_reason": "changed", "status_code": 200, "record_count": 3},
        ]
    )

    assert summary["source_count"] == 3
    assert summary["changed_count"] == 1
    assert summary["unchanged_count"] == 2
    assert summary["status_code_counts"] == {"200": 2, "304": 1}
    assert summary["short_circuit_reason_counts"] == {
        "changed": 1,
        "not_modified_304": 1,
        "same_payload_hash": 1,
    }
