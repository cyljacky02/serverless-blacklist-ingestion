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

from lookup_api import build_lookup_candidates  # noqa: E402
from sync_lookup_index import indicator_lookup_key, prepare_lookup_item  # noqa: E402


def test_build_lookup_candidates_for_url_includes_domain_fallback() -> None:
    candidates = build_lookup_candidates({"url": "example.com/path"})

    assert candidates == [
        {
            "indicator_type": "url",
            "indicator": "https://example.com/path",
            "indicator_key": indicator_lookup_key("url", "https://example.com/path"),
            "matched_via": "url",
        },
        {
            "indicator_type": "domain",
            "indicator": "example.com",
            "indicator_key": indicator_lookup_key("domain", "example.com"),
            "matched_via": "domain_from_url",
        },
    ]


def test_build_lookup_candidates_for_domain_keeps_exact_domain() -> None:
    candidates = build_lookup_candidates({"domain": "Sub.Example.Com"})

    assert candidates == [
        {
            "indicator_type": "domain",
            "indicator": "sub.example.com",
            "indicator_key": indicator_lookup_key("domain", "sub.example.com"),
            "matched_via": "domain",
        }
    ]


def test_build_lookup_candidates_for_url_like_indicator_does_not_add_bogus_raw_domain() -> None:
    candidates = build_lookup_candidates({"indicator": "https://openphish.com/"})

    assert candidates == [
        {
            "indicator_type": "url",
            "indicator": "https://openphish.com/",
            "indicator_key": indicator_lookup_key("url", "https://openphish.com/"),
            "matched_via": "inferred_url",
        },
        {
            "indicator_type": "domain",
            "indicator": "openphish.com",
            "indicator_key": indicator_lookup_key("domain", "openphish.com"),
            "matched_via": "domain_from_url",
        },
    ]


def test_prepare_lookup_item_keeps_query_relevant_fields() -> None:
    item = prepare_lookup_item(
        {
            "indicator_type": "url",
            "indicator": "https://example.com/",
            "record_statuses": ["active"],
            "source_ids": ["tw_165_fake_investment_articles"],
            "source_record_ids": ["article:1:1:1"],
            "brands": ["Example"],
            "last_retrieved_at": "2026-03-26T00:00:00Z",
        },
        run_id="run-123",
        generated_at="2026-03-26T01:00:00Z",
    )

    assert item["indicator_key"] == indicator_lookup_key("url", "https://example.com/")
    assert item["run_id"] == "run-123"
    assert item["generated_at"] == "2026-03-26T01:00:00Z"


def test_indicator_lookup_key_is_hashed_and_stable() -> None:
    key = indicator_lookup_key("url", "https://example.com/")
    assert len(key) == 64
    assert key == indicator_lookup_key("url", "https://example.com/")
