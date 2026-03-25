from __future__ import annotations

import json
import os
import sys
from pathlib import Path

os.environ.setdefault("ARTIFACTS_BUCKET", "unit-test-bucket")
os.environ.setdefault("STATE_TABLE", "unit-test-table")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "handlers"))

from collect_source import (  # noqa: E402
    _parse_title_date_range,
    _records_sha256,
    _tw165_article_records,
    _tw165_stopped_records,
    _tw165_url_records,
    handler,
)
from common import sha256_hex  # noqa: E402


def test_parse_title_date_range_supports_missing_end_year() -> None:
    start_date, end_date = _parse_title_date_range("109/03/26-04/1民眾通報假投資(博奕)詐騙網站")
    assert start_date == "2020-03-26"
    assert end_date == "2020-04-01"


def test_tw165_article_records_extracts_rows_and_dates() -> None:
    body = json.dumps(
        [
            {
                "id": 1753,
                "title": "115/3/12-115/3/18民眾通報假投資(博弈)詐騙網站",
                "publishDate": "2026-03-24T10:23:00+08:00",
                "lastUpdTm": None,
                "content": """
                    <p>intro</p>
                    <table>
                        <caption>115/3/12-115/3/18民眾通報假投資(博弈)詐騙網站</caption>
                        <tr><th>網站名稱</th><th>網址</th><th>件數</th></tr>
                        <tr><th>TimeeCoin</th><td>contimee.net/wap</td><td>10</td></tr>
                        <tr><th>BREV</th><td>www.dashit-tw.one</td><td>8</td></tr>
                    </table>
                """,
            }
        ],
        ensure_ascii=False,
    ).encode("utf-8")
    source = {
        "source_id": "tw_165_fake_investment_articles",
        "display_name": "Taiwan 165 Fake Investment and Gambling Articles",
        "metadata_url": "https://165.npa.gov.tw/#/articles/subclass/3",
        "license_url": None,
    }

    records = _tw165_article_records(body, source, "2026-03-26T00:00:00Z")

    assert len(records) == 2
    assert records[0]["indicator"] == "https://contimee.net/wap"
    assert records[0]["attributes"]["domain"] == "contimee.net"
    assert records[0]["attributes"]["case_count"] == 10
    assert records[0]["attributes"]["stat_start_date"] == "2026-03-12"
    assert records[0]["attributes"]["stat_end_date"] == "2026-03-18"
    assert records[1]["target_brand"] == "BREV"


def test_tw165_article_records_split_multi_url_cells() -> None:
    body = json.dumps(
        [
            {
                "id": 400,
                "title": "114/1/1-114/1/7民眾通報假投資(博弈)詐騙網站",
                "publishDate": "2025-01-08T10:00:00+08:00",
                "lastUpdTm": None,
                "content": """
                    <table>
                        <tr><th>網站名稱</th><th>網址</th><th>件數</th></tr>
                        <tr><th>Multi</th><td>aaa.22233349.com、https://cai.3245016.com</td><td>2</td></tr>
                    </table>
                """,
            }
        ],
        ensure_ascii=False,
    ).encode("utf-8")
    source = {
        "source_id": "tw_165_fake_investment_articles",
        "display_name": "Taiwan 165 Fake Investment and Gambling Articles",
        "metadata_url": "https://165.npa.gov.tw/#/articles/subclass/3",
        "license_url": None,
    }

    records = _tw165_article_records(body, source, "2026-03-26T00:00:00Z")

    assert len(records) == 2
    assert [record["indicator"] for record in records] == [
        "https://aaa.22233349.com/",
        "https://cai.3245016.com/",
    ]


def test_tw165_url_records_skip_embedded_header_row() -> None:
    body = (
        "\ufeffWEBSITE_NM,WEBURL,CNT,STA_SDATE,STA_EDATE\n"
        "網站名稱,網址,件數,統計起始日期,統計結束日期\n"
        "Alpha,alpha.example/path,3,2025/12/24,2025/12/31\n"
    ).encode("utf-8")
    source = {
        "source_id": "tw_165_fake_investment_sites",
        "display_name": "Taiwan 165 Fake Investment and Gambling Sites",
        "metadata_url": "https://data.gov.tw/dataset/160055",
        "license_url": "https://data.gov.tw/license",
    }

    records = _tw165_url_records(body, source, "2026-03-26T00:00:00Z")

    assert len(records) == 1
    assert records[0]["indicator"] == "https://alpha.example/path"
    assert records[0]["attributes"]["case_count"] == 3
    assert records[0]["attributes"]["stat_end_date"] == "2025-12-31"


def test_tw165_stopped_records_use_utf8_headers() -> None:
    body = (
        "\ufeff民國年月,網域,網站性質,法律依據,聲請單位\n"
        "11412,bbhhshf.cc,金融保險,詐欺犯罪危害防制條例,刑事警察局詐欺犯罪防制中心\n"
    ).encode("utf-8")
    source = {
        "source_id": "tw_165_stopped_domains",
        "display_name": "Taiwan 165 Stopped Fraud Domains",
        "metadata_url": "https://data.gov.tw/dataset/176455",
        "license_url": "https://data.gov.tw/license",
    }

    records = _tw165_stopped_records(body, source, "2026-03-26T00:00:00Z")

    assert len(records) == 1
    assert records[0]["indicator"] == "bbhhshf.cc"
    assert records[0]["source_timestamp"] == "2025-12"
    assert records[0]["attributes"]["site_type"] == "金融保險"


def test_handler_short_circuits_when_payload_hash_is_unchanged(monkeypatch) -> None:
    body = b"same-payload"
    source = {
        "source_id": "openphish_community",
        "display_name": "OpenPhish Community Feed",
        "url": "https://example.com/feed.txt",
        "metadata_url": "https://example.com/meta",
        "license_url": "https://example.com/license",
        "format": "txt",
    }
    state = {
        "source_id": source["source_id"],
        "display_name": source["display_name"],
        "last_payload_sha256": sha256_hex(body),
        "last_record_count": 300,
        "last_raw_key": "raw/openphish_community/prev/payload.txt",
        "last_normalized_key": "normalized/openphish_community/prev/records.jsonl.gz",
        "etag": "old-etag",
        "last_modified": "old-last-modified",
    }
    saved: dict[str, object] = {}
    put_calls: list[tuple[tuple, dict]] = []

    monkeypatch.setattr("collect_source.load_source_state", lambda source_id: state)
    monkeypatch.setattr(
        "collect_source.http_fetch",
        lambda url, headers=None: (
            200,
            {"Content-Type": "text/plain", "ETag": "new-etag", "Last-Modified": "new-last-modified"},
            body,
            url,
        ),
    )
    monkeypatch.setattr("collect_source.save_source_state", lambda source_id, attributes: saved.update(attributes))
    monkeypatch.setattr("collect_source.put_object", lambda *args, **kwargs: put_calls.append((args, kwargs)))

    result = handler({"source": source, "run_id": "run-1"}, None)

    assert result == {
        "source_id": "openphish_community",
        "display_name": "OpenPhish Community Feed",
        "metadata_url": "https://example.com/meta",
        "license_url": "https://example.com/license",
        "changed": False,
        "short_circuit_reason": "same_payload_hash",
        "status_code": 200,
        "record_count": 300,
        "normalized_key": "normalized/openphish_community/prev/records.jsonl.gz",
        "raw_key": "raw/openphish_community/prev/payload.txt",
        "requested_url": "https://example.com/feed.txt",
        "final_url": "https://example.com/feed.txt",
        "fetched_at": result["fetched_at"],
    }
    assert put_calls == []
    assert saved["last_payload_sha256"] == sha256_hex(body)
    assert saved["etag"] == "new-etag"
    assert saved["last_modified"] == "new-last-modified"


def test_handler_short_circuits_when_records_hash_is_unchanged(monkeypatch) -> None:
    body = b"new-payload-with-different-bytes"
    source = {
        "source_id": "openphish_community",
        "display_name": "OpenPhish Community Feed",
        "url": "https://example.com/feed.txt",
        "metadata_url": "https://example.com/meta",
        "license_url": "https://example.com/license",
        "format": "txt",
    }
    records = [
        {
            "source_id": "openphish_community",
            "source_name": "OpenPhish Community Feed",
            "indicator_type": "url",
            "indicator": "https://example.com/",
            "source_record_id": "https://example.com/",
            "record_status": "active",
            "target_brand": None,
            "source_timestamp": None,
            "retrieved_at": "2026-03-26T00:00:00Z",
            "metadata_url": "https://example.com/meta",
            "license_url": "https://example.com/license",
            "attributes": {},
        }
    ]
    state = {
        "source_id": source["source_id"],
        "display_name": source["display_name"],
        "last_payload_sha256": "different-old-payload-hash",
        "last_records_sha256": _records_sha256(records),
        "last_record_count": 1,
        "last_raw_key": "raw/openphish_community/prev/payload.txt",
        "last_normalized_key": "normalized/openphish_community/prev/records.jsonl.gz",
    }
    saved: dict[str, object] = {}
    put_calls: list[tuple[tuple, dict]] = []

    monkeypatch.setattr("collect_source.load_source_state", lambda source_id: state)
    monkeypatch.setattr(
        "collect_source.http_fetch",
        lambda url, headers=None: (
            200,
            {"Content-Type": "text/plain", "ETag": "new-etag", "Last-Modified": "new-last-modified"},
            body,
            url,
        ),
    )
    monkeypatch.setattr("collect_source._parse_records", lambda source_arg, body_arg, now_arg: records)
    monkeypatch.setattr("collect_source.save_source_state", lambda source_id, attributes: saved.update(attributes))
    monkeypatch.setattr("collect_source.put_object", lambda *args, **kwargs: put_calls.append((args, kwargs)))

    result = handler({"source": source, "run_id": "run-2"}, None)

    assert result == {
        "source_id": "openphish_community",
        "display_name": "OpenPhish Community Feed",
        "metadata_url": "https://example.com/meta",
        "license_url": "https://example.com/license",
        "changed": False,
        "short_circuit_reason": "same_records_hash",
        "status_code": 200,
        "record_count": 1,
        "normalized_key": "normalized/openphish_community/prev/records.jsonl.gz",
        "raw_key": "raw/openphish_community/prev/payload.txt",
        "requested_url": "https://example.com/feed.txt",
        "final_url": "https://example.com/feed.txt",
        "fetched_at": result["fetched_at"],
    }
    assert put_calls == []
    assert saved["last_payload_sha256"] == sha256_hex(body)
    assert saved["last_records_sha256"] == _records_sha256(records)
