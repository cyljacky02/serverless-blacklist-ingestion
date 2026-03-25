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

from sync_lookup_index import LOOKUP_METADATA_KEY, _removed_lookup_keys, prepare_lookup_item  # noqa: E402


def test_prepare_lookup_item_supports_changed_after_record() -> None:
    item = prepare_lookup_item(
        {
            "indicator_type": "domain",
            "indicator": "example.com",
            "record_statuses": ["blocked"],
            "source_ids": ["tw_165_stopped_domains"],
            "source_record_ids": ["example.com"],
            "brands": [],
            "last_retrieved_at": "2026-03-26T00:00:00Z",
        },
        run_id="run-xyz",
        generated_at="2026-03-26T00:01:00Z",
    )

    assert item["indicator"] == "example.com"
    assert item["record_statuses"] == ["blocked"]
    assert item["run_id"] == "run-xyz"


def test_removed_lookup_keys_match_indicator_identity() -> None:
    keys = _removed_lookup_keys(
        [
            {"indicator_type": "url", "indicator": "https://example.com/"},
            {"indicator_type": "domain", "indicator": "example.com"},
        ]
    )

    assert len(keys) == 2
    assert keys[0] != keys[1]


def test_lookup_metadata_key_is_stable() -> None:
    assert LOOKUP_METADATA_KEY == "__meta__#lookup_index"
