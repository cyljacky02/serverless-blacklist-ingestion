from __future__ import annotations

import gzip
import json
import re
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse

from common import (
    PHISHTANK_APP_KEY,
    SKIP_SOURCE_HOSTS,
    build_common_headers,
    csv_rows_from_bytes,
    gzip_jsonl,
    http_fetch,
    json_dumps,
    normalize_domain,
    normalize_url,
    put_object,
    sha256_hex,
    source_normalized_key,
    source_raw_key,
    utc_now_iso,
)
from state_store import load_source_state, save_source_state

DATE_RANGE_PATTERN = re.compile(
    r"(?P<start_year>\d{2,3})/(?P<start_month>\d{1,2})/(?P<start_day>\d{1,2})"
    r"\s*-\s*(?:(?P<end_year>\d{2,3})/)?(?P<end_month>\d{1,2})/(?P<end_day>\d{1,2})"
)
URL_CANDIDATE_PATTERN = re.compile(
    r"(?:(?:https?://|https//|http//)[^\s,，、]+|(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?:/[^\s,，、]*)?)",
    re.IGNORECASE,
)


class _FirstTableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._table_depth = 0
        self._found_table = False
        self._inside_caption = False
        self._inside_cell = False
        self._caption_chunks: list[str] = []
        self._cell_chunks: list[str] = []
        self._current_row: list[str] = []
        self.rows: list[list[str]] = []

    @property
    def caption(self) -> str:
        return _clean_text("".join(self._caption_chunks))

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            if not self._found_table:
                self._found_table = True
                self._table_depth = 1
                return
            if self._table_depth > 0:
                self._table_depth += 1
                return
        if self._table_depth == 0:
            return
        if tag == "caption":
            self._inside_caption = True
            return
        if tag == "tr":
            self._current_row = []
            return
        if tag in {"th", "td"}:
            self._inside_cell = True
            self._cell_chunks = []
            return
        if tag == "br" and self._inside_cell:
            self._cell_chunks.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag == "table" and self._table_depth > 0:
            self._table_depth -= 1
            return
        if self._table_depth == 0:
            return
        if tag == "caption":
            self._inside_caption = False
            return
        if tag in {"th", "td"} and self._inside_cell:
            self._current_row.append(_clean_text("".join(self._cell_chunks)))
            self._cell_chunks = []
            self._inside_cell = False
            return
        if tag == "tr" and self._current_row:
            self.rows.append(self._current_row)
            self._current_row = []

    def handle_data(self, data: str) -> None:
        if self._table_depth == 0:
            return
        if self._inside_caption:
            self._caption_chunks.append(data)
        if self._inside_cell:
            self._cell_chunks.append(data)


def _clean_text(value: Any) -> str:
    text = str(value or "")
    text = text.replace("\xa0", " ").replace("／", "/").replace("．", ".").replace("。", ".")
    return re.sub(r"\s+", " ", text).strip(" \t\r\n|;,，；")


def _coerce_int(value: Any) -> int | None:
    digits = re.sub(r"[^\d]", "", _clean_text(value))
    return int(digits) if digits else None


def _roc_to_iso(year: str, month: str, day: str) -> str:
    return f"{int(year) + 1911:04d}-{int(month):02d}-{int(day):02d}"


def _roc_month_to_iso(value: str) -> str | None:
    roc_month = _clean_text(value)
    if not roc_month.isdigit() or len(roc_month) not in {5, 6}:
        return None
    year = roc_month[:-2]
    month = roc_month[-2:]
    return f"{int(year) + 1911:04d}-{int(month):02d}"


def _parse_title_date_range(title: str) -> tuple[str | None, str | None]:
    match = DATE_RANGE_PATTERN.search(_clean_text(title))
    if not match:
        return None, None
    end_year = match.group("end_year") or match.group("start_year")
    start_date = _roc_to_iso(match.group("start_year"), match.group("start_month"), match.group("start_day"))
    end_date = _roc_to_iso(end_year, match.group("end_month"), match.group("end_day"))
    return start_date, end_date


def _canonicalize_tw165_url(value: str) -> tuple[str, str]:
    candidate = _clean_text(value)
    if not candidate:
        raise ValueError("URL is empty")
    if "://" not in candidate:
        candidate = f"https://{candidate.lstrip('/')}"
    indicator = normalize_url(candidate)
    parsed = urlparse(indicator)
    return indicator, parsed.netloc.lower()


def _extract_tw165_url_candidates(value: str) -> list[str]:
    raw_value = _clean_text(value).replace("（", "(").replace("）", ")")
    raw_value = re.sub(r"(?<=[A-Za-z0-9/])及(?=[A-Za-z0-9])", " ", raw_value)
    candidates: list[str] = []
    seen: set[str] = set()
    for match in URL_CANDIDATE_PATTERN.finditer(raw_value):
        token = match.group(0).strip("()[]{}<>.,;，；、")
        token = token.rstrip(")")
        if not token:
            continue
        if token.lower() in {"app", "https://", "http://"}:
            continue
        if token.startswith("https//") and not token.startswith("https://"):
            token = f"https://{token[len('https//'):]}"
        if token.startswith("http//") and not token.startswith("http://"):
            token = f"http://{token[len('http//'):]}"
        if token in seen:
            continue
        seen.add(token)
        candidates.append(token)
    if candidates:
        return candidates
    return [raw_value] if raw_value else []


def _effective_url(source: dict[str, Any]) -> str:
    if source["source_id"] != "phishtank_online_valid" or not PHISHTANK_APP_KEY:
        return source["url"]
    return f"https://data.phishtank.com/data/{PHISHTANK_APP_KEY}/online-valid.csv.gz"


def _source_should_be_skipped(url: str) -> bool:
    hostname = (urlparse(url).hostname or "").strip().lower()
    return bool(hostname and hostname in SKIP_SOURCE_HOSTS)


def _raw_extension(source: dict[str, Any]) -> str:
    format_name = source.get("format", "")
    if format_name.endswith(".gz"):
        return ".gz"
    if format_name == "json":
        return ".json"
    if format_name == "txt":
        return ".txt"
    if format_name == "csv":
        return ".csv"
    return ".bin"


def _openphish_records(body: bytes, source: dict[str, Any], retrieved_at: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for line in body.decode("utf-8", errors="ignore").splitlines():
        candidate = line.strip()
        if not candidate or candidate.startswith("#"):
            continue
        try:
            indicator = normalize_url(candidate)
        except ValueError:
            continue
        records.append(
            {
                "source_id": source["source_id"],
                "source_name": source["display_name"],
                "indicator_type": "url",
                "indicator": indicator,
                "source_record_id": indicator,
                "record_status": "active",
                "target_brand": None,
                "source_timestamp": None,
                "retrieved_at": retrieved_at,
                "metadata_url": source["metadata_url"],
                "license_url": source["license_url"],
                "attributes": {},
            }
        )
    return records


def _phishtank_records(body: bytes, source: dict[str, Any], retrieved_at: str) -> list[dict[str, Any]]:
    rows = csv_rows_from_bytes(gzip.decompress(body))
    records: list[dict[str, Any]] = []
    for row in rows:
        url_value = row.get("url", "").strip()
        if not url_value:
            continue
        try:
            indicator = normalize_url(url_value)
        except ValueError:
            continue
        records.append(
            {
                "source_id": source["source_id"],
                "source_name": source["display_name"],
                "indicator_type": "url",
                "indicator": indicator,
                "source_record_id": row.get("phish_id") or indicator,
                "record_status": "active" if row.get("online", "").lower() == "yes" else "inactive",
                "target_brand": row.get("target") or None,
                "source_timestamp": row.get("verification_time") or row.get("submission_time") or None,
                "retrieved_at": retrieved_at,
                "metadata_url": source["metadata_url"],
                "license_url": source["license_url"],
                "attributes": {
                    "phish_detail_url": row.get("phish_detail_url"),
                    "submission_time": row.get("submission_time"),
                    "verification_time": row.get("verification_time"),
                    "verified": row.get("verified"),
                },
            }
        )
    return records


def _tw165_article_records(body: bytes, source: dict[str, Any], retrieved_at: str) -> list[dict[str, Any]]:
    payload = json.loads(body.decode("utf-8"))
    records: list[dict[str, Any]] = []
    for article in payload:
        parser = _FirstTableParser()
        parser.feed(article.get("content") or "")
        parser.close()
        stat_start_date, stat_end_date = _parse_title_date_range(article.get("title", ""))
        publish_date = article.get("publishDate") or None
        last_updated = article.get("lastUpdTm") or None
        for row_index, cells in enumerate(parser.rows, start=1):
            if len(cells) < 2:
                continue
            website_name = _clean_text(cells[0])
            raw_url = _clean_text(cells[1])
            if website_name == "網站名稱" or raw_url == "網址":
                continue
            for token_index, url_candidate in enumerate(_extract_tw165_url_candidates(raw_url), start=1):
                try:
                    indicator, domain = _canonicalize_tw165_url(url_candidate)
                except ValueError:
                    continue
                records.append(
                    {
                        "source_id": source["source_id"],
                        "source_name": source["display_name"],
                        "indicator_type": "url",
                        "indicator": indicator,
                        "source_record_id": f"article:{article.get('id')}:{row_index}:{token_index}",
                        "record_status": "active",
                        "target_brand": website_name or None,
                        "source_timestamp": stat_end_date or publish_date,
                        "retrieved_at": retrieved_at,
                        "metadata_url": source["metadata_url"],
                        "license_url": source["license_url"],
                        "attributes": {
                            "article_id": article.get("id"),
                            "article_title": _clean_text(article.get("title")),
                            "article_url": f"https://165.npa.gov.tw/#/article/{article.get('id')}",
                            "publish_date": publish_date,
                            "last_updated": last_updated,
                            "case_count": _coerce_int(cells[2] if len(cells) > 2 else None),
                            "stat_start_date": stat_start_date,
                            "stat_end_date": stat_end_date,
                            "domain": domain,
                            "caption": parser.caption or None,
                            "raw_url": raw_url,
                            "url_token": url_candidate,
                        },
                    }
                )
    return records


def _tw165_stopped_records(body: bytes, source: dict[str, Any], retrieved_at: str) -> list[dict[str, Any]]:
    rows = csv_rows_from_bytes(body)
    records: list[dict[str, Any]] = []
    for row in rows:
        domain_value = _clean_text(row.get("網域"))
        if domain_value == "網域" or not domain_value:
            continue
        try:
            indicator = normalize_domain(domain_value)
        except ValueError:
            continue
        records.append(
            {
                "source_id": source["source_id"],
                "source_name": source["display_name"],
                "indicator_type": "domain",
                "indicator": indicator,
                "source_record_id": indicator,
                "record_status": "blocked",
                "target_brand": None,
                "source_timestamp": _roc_month_to_iso(row.get("民國年月", "")),
                "retrieved_at": retrieved_at,
                "metadata_url": source["metadata_url"],
                "license_url": source["license_url"],
                "attributes": {
                    "site_type": _clean_text(row.get("網站性質")) or None,
                    "legal_basis": _clean_text(row.get("法律依據")) or None,
                    "requesting_agency": _clean_text(row.get("聲請單位")) or None,
                    "roc_month": _clean_text(row.get("民國年月")) or None,
                },
            }
        )
    return records


def _tw165_url_records(body: bytes, source: dict[str, Any], retrieved_at: str) -> list[dict[str, Any]]:
    rows = csv_rows_from_bytes(body)
    records: list[dict[str, Any]] = []
    for row in rows:
        website_name = _clean_text(row.get("WEBSITE_NM"))
        url_value = _clean_text(row.get("WEBURL"))
        if website_name == "網站名稱" or url_value == "網址" or not url_value:
            continue
        try:
            indicator, domain = _canonicalize_tw165_url(url_value)
        except ValueError:
            continue
        records.append(
            {
                "source_id": source["source_id"],
                "source_name": source["display_name"],
                "indicator_type": "url",
                "indicator": indicator,
                "source_record_id": indicator,
                "record_status": "active",
                "target_brand": website_name or None,
                "source_timestamp": (_clean_text(row.get("STA_EDATE")) or _clean_text(row.get("STA_SDATE"))).replace("/", "-")
                or None,
                "retrieved_at": retrieved_at,
                "metadata_url": source["metadata_url"],
                "license_url": source["license_url"],
                "attributes": {
                    "case_count": _coerce_int(row.get("CNT")),
                    "stat_start_date": _clean_text(row.get("STA_SDATE")).replace("/", "-") or None,
                    "stat_end_date": _clean_text(row.get("STA_EDATE")).replace("/", "-") or None,
                    "domain": domain,
                    "raw_url": url_value,
                },
            }
        )
    return records


def _parse_records(source: dict[str, Any], body: bytes, retrieved_at: str) -> list[dict[str, Any]]:
    source_id = source["source_id"]
    if source_id == "openphish_community":
        return _openphish_records(body, source, retrieved_at)
    if source_id == "phishtank_online_valid":
        return _phishtank_records(body, source, retrieved_at)
    if source_id == "tw_165_fake_investment_articles":
        return _tw165_article_records(body, source, retrieved_at)
    if source_id == "tw_165_stopped_domains":
        return _tw165_stopped_records(body, source, retrieved_at)
    if source_id == "tw_165_fake_investment_sites":
        return _tw165_url_records(body, source, retrieved_at)
    raise ValueError(f"Unsupported source_id: {source_id}")


def _records_sha256(records: list[dict[str, Any]]) -> str:
    payload = json.dumps(records, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256_hex(payload)


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    source = event["source"]
    source_id = source["source_id"]
    run_id = event["run_id"]
    now = utc_now_iso()
    state = load_source_state(source_id)

    url = _effective_url(source)
    if _source_should_be_skipped(url):
        save_source_state(
            source_id,
            {
                **state,
                "source_id": source_id,
                "display_name": source["display_name"],
                "last_checked_at": now,
                "last_requested_url": url,
                "last_final_url": url,
                "source_skip_reason": "configured_host_skip",
                "source_skip_host": (urlparse(url).hostname or "").strip().lower(),
                "metadata_url": source["metadata_url"],
                "license_url": source["license_url"],
            },
        )
        return {
            "source_id": source_id,
            "display_name": source["display_name"],
            "metadata_url": source["metadata_url"],
            "license_url": source["license_url"],
            "changed": False,
            "short_circuit_reason": "source_disabled",
            "status_code": None,
            "record_count": state.get("last_record_count", 0),
            "normalized_key": state.get("last_normalized_key"),
            "raw_key": state.get("last_raw_key"),
            "requested_url": url,
            "final_url": url,
            "fetched_at": now,
        }

    status_code, headers, body, final_url = http_fetch(url, headers=build_common_headers(state))

    if status_code == 304:
        save_source_state(
            source_id,
            {
                **state,
                "source_id": source_id,
                "last_checked_at": now,
                "last_http_status": status_code,
                "last_requested_url": url,
                "last_final_url": final_url,
            },
        )
        return {
            "source_id": source_id,
            "display_name": source["display_name"],
            "metadata_url": source["metadata_url"],
            "license_url": source["license_url"],
            "changed": False,
            "short_circuit_reason": "not_modified_304",
            "status_code": status_code,
            "record_count": state.get("last_record_count", 0),
            "normalized_key": state.get("last_normalized_key"),
            "raw_key": state.get("last_raw_key"),
            "requested_url": url,
            "final_url": final_url,
            "fetched_at": now,
        }

    if status_code >= 400:
        raise RuntimeError(f"{source_id} fetch failed with HTTP {status_code} from {final_url}")

    payload_sha256 = sha256_hex(body)
    if (
        payload_sha256 == state.get("last_payload_sha256")
        and state.get("last_normalized_key")
        and state.get("last_raw_key")
    ):
        save_source_state(
            source_id,
            {
                **state,
                "source_id": source_id,
                "display_name": source["display_name"],
                "last_checked_at": now,
                "last_http_status": status_code,
                "last_requested_url": url,
                "last_final_url": final_url,
                "etag": headers.get("ETag"),
                "last_modified": headers.get("Last-Modified"),
                "last_payload_sha256": payload_sha256,
                "metadata_url": source["metadata_url"],
                "license_url": source["license_url"],
            },
        )
        return {
            "source_id": source_id,
            "display_name": source["display_name"],
            "metadata_url": source["metadata_url"],
            "license_url": source["license_url"],
            "changed": False,
            "short_circuit_reason": "same_payload_hash",
            "status_code": status_code,
            "record_count": state.get("last_record_count", 0),
            "normalized_key": state.get("last_normalized_key"),
            "raw_key": state.get("last_raw_key"),
            "requested_url": url,
            "final_url": final_url,
            "fetched_at": now,
        }

    records = _parse_records(source, body, now)
    records_sha256 = _records_sha256(records)
    if (
        records_sha256 == state.get("last_records_sha256")
        and state.get("last_normalized_key")
        and state.get("last_raw_key")
    ):
        save_source_state(
            source_id,
            {
                **state,
                "source_id": source_id,
                "display_name": source["display_name"],
                "last_checked_at": now,
                "last_http_status": status_code,
                "last_requested_url": url,
                "last_final_url": final_url,
                "etag": headers.get("ETag"),
                "last_modified": headers.get("Last-Modified"),
                "last_payload_sha256": payload_sha256,
                "last_records_sha256": records_sha256,
                "metadata_url": source["metadata_url"],
                "license_url": source["license_url"],
            },
        )
        return {
            "source_id": source_id,
            "display_name": source["display_name"],
            "metadata_url": source["metadata_url"],
            "license_url": source["license_url"],
            "changed": False,
            "short_circuit_reason": "same_records_hash",
            "status_code": status_code,
            "record_count": state.get("last_record_count", len(records)),
            "normalized_key": state.get("last_normalized_key"),
            "raw_key": state.get("last_raw_key"),
            "requested_url": url,
            "final_url": final_url,
            "fetched_at": now,
        }

    raw_key = source_raw_key(source_id, run_id, _raw_extension(source))
    put_object(
        raw_key,
        body,
        content_type=headers.get("Content-Type", "application/octet-stream"),
        metadata={
            "source-id": source_id,
            "fetched-at": now,
        },
    )

    normalized_key = source_normalized_key(source_id, run_id)
    put_object(
        normalized_key,
        gzip_jsonl(records),
        content_type="application/x-ndjson",
        content_encoding="gzip",
        metadata={
            "source-id": source_id,
            "record-count": str(len(records)),
            "fetched-at": now,
        },
    )

    snapshot = {
        "source_id": source_id,
        "display_name": source["display_name"],
        "last_checked_at": now,
        "last_fetched_at": now,
        "last_http_status": status_code,
        "last_requested_url": url,
        "last_final_url": final_url,
        "etag": headers.get("ETag"),
        "last_modified": headers.get("Last-Modified"),
        "last_payload_sha256": payload_sha256,
        "last_records_sha256": records_sha256,
        "last_record_count": len(records),
        "last_raw_key": raw_key,
        "last_normalized_key": normalized_key,
        "metadata_url": source["metadata_url"],
        "license_url": source["license_url"],
    }
    save_source_state(source_id, snapshot)

    put_object(
        f"{normalized_key}.summary.json",
        json_dumps(
            {
                "source_id": source_id,
                "display_name": source["display_name"],
                "record_count": len(records),
                "fetched_at": now,
                "requested_url": url,
                "final_url": final_url,
                "status_code": status_code,
            }
        ),
        content_type="application/json",
    )

    return {
        "source_id": source_id,
        "display_name": source["display_name"],
        "metadata_url": source["metadata_url"],
        "license_url": source["license_url"],
        "changed": True,
        "short_circuit_reason": "changed",
        "status_code": status_code,
        "record_count": len(records),
        "normalized_key": normalized_key,
        "raw_key": raw_key,
        "requested_url": url,
        "final_url": final_url,
        "fetched_at": now,
    }
