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

import lookup_store  # noqa: E402
import sync_lookup_index  # noqa: E402
from common import LOOKUP_METADATA_KEY  # noqa: E402


class FakeBatchWriter:
    def __init__(self, table: "FakeLookupTable") -> None:
        self.table = table

    def __enter__(self) -> "FakeBatchWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def put_item(self, *, Item: dict) -> None:  # noqa: N803
        self.table.items[Item["indicator_key"]] = Item

    def delete_item(self, *, Key: dict) -> None:  # noqa: N803
        self.table.items.pop(Key["indicator_key"], None)


class FakeLookupTable:
    def __init__(self, items: dict[str, dict] | None = None, *, stale_scan_items: dict[str, dict] | None = None) -> None:
        self.items = dict(items or {})
        self.stale_scan_items = dict(stale_scan_items or self.items)
        self.name = "unit-test-lookup-table"
        self.scan_calls: list[dict[str, object]] = []
        self.get_item_calls: list[dict[str, object]] = []

    def batch_writer(self, *, overwrite_by_pkeys: list[str]) -> FakeBatchWriter:
        return FakeBatchWriter(self)

    def get_item(self, *, Key: dict, ConsistentRead: bool = False) -> dict:  # noqa: N803
        self.get_item_calls.append({"Key": Key, "ConsistentRead": ConsistentRead})
        item = self.items.get(Key["indicator_key"])
        return {"Item": item} if item else {}

    def put_item(self, *, Item: dict) -> None:  # noqa: N803
        self.items[Item["indicator_key"]] = Item

    def scan(  # noqa: N803
        self,
        *,
        Segment: int,
        TotalSegments: int,
        ConsistentRead: bool = False,
        ProjectionExpression: str | None = None,
        ExclusiveStartKey: dict | None = None,
    ) -> dict:
        self.scan_calls.append(
            {
                "Segment": Segment,
                "TotalSegments": TotalSegments,
                "ConsistentRead": ConsistentRead,
                "ProjectionExpression": ProjectionExpression,
                "ExclusiveStartKey": ExclusiveStartKey,
            }
        )
        source = self.items if ConsistentRead else self.stale_scan_items
        keys = sorted(key for key in source if sum(key.encode("utf-8")) % TotalSegments == Segment)
        if ExclusiveStartKey:
            last_key = ExclusiveStartKey["indicator_key"]
            keys = [key for key in keys if key > last_key]
        items = [dict(source[key]) for key in keys]
        if ProjectionExpression:
            allowed = [part.strip() for part in ProjectionExpression.split(",")]
            items = [{key: value for key, value in item.items() if key in allowed} for item in items]
        return {"Items": items}


def test_sync_lookup_index_delta_mode_uses_manifest_delta_parts(monkeypatch) -> None:
    removed_key = sync_lookup_index.indicator_lookup_key("domain", "gone.example")
    table = FakeLookupTable(
        {
            LOOKUP_METADATA_KEY: {
                "indicator_key": LOOKUP_METADATA_KEY,
                "last_rebuild_run_id": "rebuild-1",
            },
            removed_key: {
                "indicator_key": removed_key,
                "indicator": "gone.example",
            },
        }
    )
    artifact_records = {
        "new-00": [
            {
                "indicator_type": "domain",
                "indicator": "new.example",
                "record_statuses": ["active"],
                "source_ids": ["source_a"],
                "source_record_ids": ["new1"],
                "brands": ["New"],
                "last_retrieved_at": "2026-03-27T00:00:00Z",
            }
        ],
        "changed-00": [
            {
                "indicator_type": "url",
                "indicator": "https://changed.example/",
                "after": {
                    "indicator_type": "url",
                    "indicator": "https://changed.example/",
                    "record_statuses": ["blocked"],
                    "source_ids": ["source_b"],
                    "source_record_ids": ["changed1"],
                    "brands": ["Changed"],
                    "last_retrieved_at": "2026-03-27T00:00:00Z",
                },
            }
        ],
        "removed-00": [
            {
                "indicator_type": "domain",
                "indicator": "gone.example",
            }
        ],
    }

    monkeypatch.setattr(lookup_store, "lookup_table", table)
    monkeypatch.setattr(lookup_store, "iter_gzip_jsonl", lambda key: iter(artifact_records.get(key, [])))

    result = sync_lookup_index.handler(
        {
            "run_id": "run-delta",
            "generated_at": "2026-03-27T00:00:00Z",
            "lookup_sync_mode": "delta",
            "sources": [{"source_id": "source_a"}, {"source_id": "source_b"}],
            "combined_record_count": 2,
            "delta_counts": {"new": 1, "changed": 1, "removed": 1},
            "collection_summary": {"changed_count": 2},
            "artifacts": {
                "manifest_key": "manifest.json",
                "delta_parts": {
                    "new": {"00": "new-00"},
                    "changed": {"00": "changed-00"},
                    "removed": {"00": "removed-00"},
                },
            },
        },
        None,
    )

    new_key = sync_lookup_index.indicator_lookup_key("domain", "new.example")
    changed_key = sync_lookup_index.indicator_lookup_key("url", "https://changed.example/")

    assert removed_key not in table.items
    assert table.items[new_key]["rebuild_run_id"] == "rebuild-1"
    assert table.items[changed_key]["record_statuses"] == ["blocked"]
    assert result["lookup_sync"]["sync_mode"] == "delta"


def test_sync_lookup_index_rebuild_mode_rewrites_and_cleans_stale_rows(monkeypatch) -> None:
    current_key = sync_lookup_index.indicator_lookup_key("domain", "current.example")
    stale_key = sync_lookup_index.indicator_lookup_key("domain", "stale.example")
    initial_items = {
        LOOKUP_METADATA_KEY: {
            "indicator_key": LOOKUP_METADATA_KEY,
            "last_rebuild_run_id": "old-rebuild",
        },
        current_key: {
            "indicator_key": current_key,
            "indicator": "current.example",
            "rebuild_run_id": "old-rebuild",
        },
        stale_key: {
            "indicator_key": stale_key,
            "indicator": "stale.example",
            "rebuild_run_id": "old-rebuild",
        },
        "domain#legacy.example": {
            "indicator_key": "domain#legacy.example",
            "indicator": "legacy.example",
        },
    }
    table = FakeLookupTable(
        {
            **initial_items,
        },
        stale_scan_items=initial_items,
    )
    artifact_records = {
        "combined-00": [
            {
                "indicator_type": "domain",
                "indicator": "current.example",
                "record_statuses": ["active"],
                "source_ids": ["source_a"],
                "source_record_ids": ["current1"],
                "brands": [],
                "last_retrieved_at": "2026-03-27T00:00:00Z",
            }
        ]
    }

    monkeypatch.setattr(lookup_store, "lookup_table", table)
    monkeypatch.setattr(lookup_store, "iter_gzip_jsonl", lambda key: iter(artifact_records.get(key, [])))
    monkeypatch.setattr(lookup_store, "REBUILD_SCAN_SEGMENTS", 2)

    result = sync_lookup_index.handler(
        {
            "run_id": "run-rebuild",
            "generated_at": "2026-03-27T00:00:00Z",
            "lookup_sync_mode": "rebuild",
            "sources": [{"source_id": "source_a"}],
            "combined_record_count": 1,
            "delta_counts": {"new": 1, "changed": 0, "removed": 0},
            "collection_summary": {"changed_count": 1},
            "artifacts": {
                "manifest_key": "manifest.json",
                "combined_parts": {"00": "combined-00"},
            },
        },
        None,
    )

    assert current_key in table.items
    assert table.items[current_key]["rebuild_run_id"] == "run-rebuild"
    assert stale_key not in table.items
    assert "domain#legacy.example" not in table.items
    assert table.items[LOOKUP_METADATA_KEY]["last_rebuild_run_id"] == "run-rebuild"
    assert result["lookup_sync"]["sync_mode"] == "rebuild"
    assert all(call["ConsistentRead"] is True for call in table.scan_calls)
    assert all(call["ProjectionExpression"] == lookup_store.REBUILD_SCAN_PROJECTION for call in table.scan_calls)
