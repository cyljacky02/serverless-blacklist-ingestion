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

import merge_curated  # noqa: E402
from common import gzip_jsonl, indicator_partition_id, read_gzip_jsonl  # noqa: E402


def test_merge_curated_handler_builds_partitioned_run_artifacts(monkeypatch) -> None:
    current_records_by_key = {
        "normalized/source-a.jsonl.gz": [
            {
                "source_id": "source_a",
                "indicator_type": "url",
                "indicator": "https://a.example/",
                "record_status": "active",
                "source_record_id": "a1",
                "target_brand": "Alpha",
                "retrieved_at": "2026-03-27T00:00:00Z",
            }
        ],
        "normalized/source-b.jsonl.gz": [
            {
                "source_id": "source_b",
                "indicator_type": "url",
                "indicator": "https://a.example/",
                "record_status": "blocked",
                "source_record_id": "b1",
                "target_brand": "Alpha",
                "retrieved_at": "2026-03-27T01:00:00Z",
            },
            {
                "source_id": "source_b",
                "indicator_type": "domain",
                "indicator": "brand-new.example",
                "record_status": "active",
                "source_record_id": "new1",
                "target_brand": "New",
                "retrieved_at": "2026-03-27T01:00:00Z",
            },
        ],
    }
    previous_records = [
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
            "source_ids": ["source_old"],
            "source_record_ids": ["gone1"],
            "brands": [],
            "last_retrieved_at": "2026-03-26T00:00:00Z",
            "generated_at": "2026-03-26T00:00:00Z",
        },
    ]
    previous_manifest = {
        "run_id": "previous-run",
        "artifacts": {
            "combined_parts": {
                indicator_partition_id(previous_records[0]["indicator_type"], previous_records[0]["indicator"]): "prev-a.jsonl.gz",
                indicator_partition_id(previous_records[1]["indicator_type"], previous_records[1]["indicator"]): "prev-gone.jsonl.gz",
            }
        },
    }
    previous_bytes = {
        "prev-a.jsonl.gz": gzip_jsonl([previous_records[0]]),
        "prev-gone.jsonl.gz": gzip_jsonl([previous_records[1]]),
    }
    stored_objects: dict[str, dict[str, object]] = {}

    monkeypatch.setattr(merge_curated, "iter_gzip_jsonl", lambda key: iter(current_records_by_key[key]))
    monkeypatch.setattr(merge_curated, "load_latest_curated_metadata", lambda: {"manifest_key": "prev-manifest.json"})
    monkeypatch.setattr(merge_curated, "load_json_object", lambda key: previous_manifest if key == "prev-manifest.json" else None)
    monkeypatch.setattr(merge_curated, "try_get_object_bytes", lambda key: previous_bytes.get(key))
    monkeypatch.setattr(
        merge_curated,
        "put_object",
        lambda key, body, **kwargs: stored_objects.setdefault(key, {"body": body, **kwargs}),
    )

    result = merge_curated.handler(
        {
            "run_id": "run-123",
            "trigger": "manual",
            "requested_by": "pytest",
            "lookup_sync_mode": "delta",
            "collection_results": [
                {
                    "source_id": "source_a",
                    "display_name": "Source A",
                    "changed": True,
                    "short_circuit_reason": "changed",
                    "status_code": 200,
                    "record_count": 1,
                    "normalized_key": "normalized/source-a.jsonl.gz",
                    "raw_key": "raw/source-a.txt",
                    "metadata_url": "https://example.com/source-a",
                    "license_url": "https://example.com/license-a",
                    "fetched_at": "2026-03-27T00:00:00Z",
                },
                {
                    "source_id": "source_b",
                    "display_name": "Source B",
                    "changed": True,
                    "short_circuit_reason": "changed",
                    "status_code": 200,
                    "record_count": 2,
                    "normalized_key": "normalized/source-b.jsonl.gz",
                    "raw_key": "raw/source-b.txt",
                    "metadata_url": "https://example.com/source-b",
                    "license_url": "https://example.com/license-b",
                    "fetched_at": "2026-03-27T01:00:00Z",
                },
            ],
        },
        None,
    )

    combined_records: list[dict] = []
    for key in result["artifacts"]["combined_parts"].values():
        combined_records.extend(read_gzip_jsonl(stored_objects[key]["body"]))
    combined_records.sort(key=lambda item: (item["indicator_type"], item["indicator"]))

    assert result["previous_published_run_id"] == "previous-run"
    assert result["delta_counts"] == {"new": 1, "changed": 1, "removed": 1}
    assert combined_records == [
        {
            "indicator_type": "domain",
            "indicator": "brand-new.example",
            "record_statuses": ["active"],
            "source_ids": ["source_b"],
            "source_record_ids": ["new1"],
            "brands": ["New"],
            "last_retrieved_at": "2026-03-27T01:00:00Z",
            "generated_at": result["generated_at"],
        },
        {
            "indicator_type": "url",
            "indicator": "https://a.example/",
            "record_statuses": ["active", "blocked"],
            "source_ids": ["source_a", "source_b"],
            "source_record_ids": ["a1", "b1"],
            "brands": ["Alpha"],
            "last_retrieved_at": "2026-03-27T01:00:00Z",
            "generated_at": result["generated_at"],
        },
    ]
    assert result["artifacts"]["manifest_key"].startswith("curated/runs/run-123/")
