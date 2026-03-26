from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from common import curated_key, json_dumps, put_object, utc_now_iso
from contracts import CuratedLatestPointer, PublishedLatestPointer
from state_store import save_latest_curated_pointer


logger = logging.getLogger(__name__)

S3_POINTER_WRITE_ATTEMPTS = int(os.environ.get("PUBLISH_LATEST_S3_WRITE_ATTEMPTS", "3"))
S3_POINTER_WRITE_SLEEP_SECONDS = float(os.environ.get("PUBLISH_LATEST_S3_WRITE_SLEEP_SECONDS", "1"))


def _log_publish_event(payload: dict[str, Any], *, warning: bool = False) -> None:
    level = logging.WARNING if warning else logging.INFO
    logger.log(level, json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str))


def _build_pointer(event: dict[str, Any], *, published_at: str) -> CuratedLatestPointer:
    manifest_key = str(event["artifacts"]["manifest_key"])
    return {
        "schema_version": event.get("schema_version"),
        "run_id": event.get("run_id"),
        "generated_at": event.get("generated_at"),
        "published_at": published_at,
        "manifest_key": manifest_key,
        "combined_record_count": event.get("combined_record_count"),
        "delta_counts": event.get("delta_counts") or {},
        "source_count": event.get("source_count"),
    }


def _write_s3_pointer(pointer: CuratedLatestPointer) -> int | None:
    last_error: Exception | None = None
    for attempt in range(1, S3_POINTER_WRITE_ATTEMPTS + 1):
        try:
            put_object(
                curated_key("manifest.json"),
                json_dumps(pointer),
                content_type="application/json",
                metadata={
                    "run-id": str(pointer.get("run_id") or "manual"),
                    "generated-at": str(pointer.get("generated_at") or ""),
                    "published-at": pointer["published_at"],
                },
            )
            return attempt
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < S3_POINTER_WRITE_ATTEMPTS:
                time.sleep(S3_POINTER_WRITE_SLEEP_SECONDS)

    if last_error is not None:
        raise last_error
    return None


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    published_at = utc_now_iso()
    pointer = _build_pointer(event, published_at=published_at)
    save_latest_curated_pointer(pointer)

    published_latest: PublishedLatestPointer = {
        **pointer,
        "authoritative_store": "dynamodb",
        "s3_pointer_written": False,
    }

    try:
        attempt_count = _write_s3_pointer(pointer)
        published_latest["s3_pointer_written"] = True
        if attempt_count is not None:
            published_latest["s3_pointer_write_attempts"] = attempt_count
        _log_publish_event(
            {
                "component": "publish_latest",
                "run_id": pointer.get("run_id"),
                "outcome": "published",
                "authoritative_store": "dynamodb",
                "s3_pointer_written": True,
                "s3_pointer_write_attempts": attempt_count,
            }
        )
    except Exception as exc:  # noqa: BLE001
        published_latest["warning"] = f"s3-pointer-write-failed: {exc}"
        _log_publish_event(
            {
                "component": "publish_latest",
                "run_id": pointer.get("run_id"),
                "outcome": "published_with_s3_warning",
                "authoritative_store": "dynamodb",
                "s3_pointer_written": False,
                "warning": str(exc),
            },
            warning=True,
        )

    return {
        **event,
        "published_latest": published_latest,
    }
