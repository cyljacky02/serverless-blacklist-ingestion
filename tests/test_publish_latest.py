from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

os.environ.setdefault("ARTIFACTS_BUCKET", "unit-test-bucket")
os.environ.setdefault("STATE_TABLE", "unit-test-table")
os.environ.setdefault("LOOKUP_TABLE", "unit-test-lookup-table")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "handlers"))

import publish_latest  # noqa: E402


def _event() -> dict[str, object]:
    return {
        "schema_version": 2,
        "run_id": "run-123",
        "generated_at": "2026-03-27T00:00:00Z",
        "combined_record_count": 10,
        "delta_counts": {"new": 2, "changed": 3, "removed": 1},
        "source_count": 5,
        "artifacts": {"manifest_key": "curated/runs/run-123/manifest.json"},
    }


def test_publish_latest_dynamodb_failure_short_circuits_before_s3(monkeypatch) -> None:
    put_calls: list[tuple[tuple, dict]] = []

    monkeypatch.setattr(publish_latest, "utc_now_iso", lambda: "2026-03-27T01:00:00Z")

    def _raise(_: dict) -> None:
        raise RuntimeError("ddb down")

    monkeypatch.setattr(publish_latest, "save_latest_curated_pointer", _raise)
    monkeypatch.setattr(publish_latest, "put_object", lambda *args, **kwargs: put_calls.append((args, kwargs)))

    with pytest.raises(RuntimeError, match="ddb down"):
        publish_latest.handler(_event(), None)

    assert put_calls == []


def test_publish_latest_s3_failure_keeps_dynamodb_authoritative(monkeypatch) -> None:
    saved_pointers: list[dict[str, object]] = []
    put_calls: list[tuple[tuple, dict]] = []

    monkeypatch.setattr(publish_latest, "utc_now_iso", lambda: "2026-03-27T01:00:00Z")
    monkeypatch.setattr(publish_latest, "S3_POINTER_WRITE_ATTEMPTS", 2)
    monkeypatch.setattr(publish_latest, "S3_POINTER_WRITE_SLEEP_SECONDS", 0)
    monkeypatch.setattr(publish_latest, "save_latest_curated_pointer", lambda pointer: saved_pointers.append(dict(pointer)))

    def _raise(*args, **kwargs) -> None:
        put_calls.append((args, kwargs))
        raise RuntimeError("s3 unavailable")

    monkeypatch.setattr(publish_latest, "put_object", _raise)

    result = publish_latest.handler(_event(), None)

    assert len(saved_pointers) == 1
    assert len(put_calls) == 2
    assert result["published_latest"]["authoritative_store"] == "dynamodb"
    assert result["published_latest"]["s3_pointer_written"] is False
    assert "warning" in result["published_latest"]


def test_publish_latest_happy_path_updates_dynamodb_then_s3(monkeypatch) -> None:
    call_order: list[str] = []
    saved_pointers: list[dict[str, object]] = []
    s3_writes: list[dict[str, object]] = []

    monkeypatch.setattr(publish_latest, "utc_now_iso", lambda: "2026-03-27T01:00:00Z")

    def _save(pointer: dict[str, object]) -> None:
        call_order.append("dynamodb")
        saved_pointers.append(dict(pointer))

    def _put_object(key: str, body: bytes, **kwargs) -> None:
        call_order.append("s3")
        s3_writes.append({"key": key, "body": body, **kwargs})

    monkeypatch.setattr(publish_latest, "save_latest_curated_pointer", _save)
    monkeypatch.setattr(publish_latest, "put_object", _put_object)

    result = publish_latest.handler(_event(), None)

    assert call_order == ["dynamodb", "s3"]
    assert result["published_latest"]["s3_pointer_written"] is True
    assert result["published_latest"]["s3_pointer_write_attempts"] == 1
    assert s3_writes[0]["key"] == "curated/latest/manifest.json"
    assert json.loads(s3_writes[0]["body"].decode("utf-8")) == saved_pointers[0]
