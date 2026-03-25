from __future__ import annotations

import base64
import json
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse

from common import LOOKUP_METADATA_KEY, indicator_lookup_key, lookup_table, normalize_domain, normalize_url


def _json_response(status_code: int, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json; charset=utf-8"},
        "body": json.dumps(_json_safe(payload), ensure_ascii=False),
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _normalize_query_url(value: str) -> str:
    candidate = value.strip()
    if "://" not in candidate:
        candidate = f"https://{candidate.lstrip('/')}"
    return normalize_url(candidate)


def build_lookup_candidates(payload: dict[str, Any]) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    seen: set[str] = set()

    def add_candidate(indicator_type: str, indicator: str, matched_via: str) -> None:
        lookup_key = indicator_lookup_key(indicator_type, indicator)
        if lookup_key in seen:
            return
        seen.add(lookup_key)
        candidates.append(
            {
                "indicator_type": indicator_type,
                "indicator": indicator,
                "indicator_key": lookup_key,
                "matched_via": matched_via,
            }
        )

    if payload.get("url"):
        normalized_url = _normalize_query_url(str(payload["url"]))
        add_candidate("url", normalized_url, "url")
        add_candidate("domain", urlparse(normalized_url).netloc.lower(), "domain_from_url")
        return candidates

    if payload.get("domain"):
        normalized_domain = normalize_domain(str(payload["domain"]))
        add_candidate("domain", normalized_domain, "domain")
        return candidates

    indicator = str(payload.get("indicator") or "").strip()
    indicator_type = str(payload.get("indicator_type") or "").strip().lower()
    if not indicator:
        return candidates

    if indicator_type == "url":
        normalized_url = _normalize_query_url(indicator)
        add_candidate("url", normalized_url, "indicator_url")
        add_candidate("domain", urlparse(normalized_url).netloc.lower(), "domain_from_url")
        return candidates

    if indicator_type == "domain":
        add_candidate("domain", normalize_domain(indicator), "indicator_domain")
        return candidates

    looks_like_url = any(token in indicator for token in ("://", "/", "?", "#"))
    if looks_like_url:
        try:
            normalized_url = _normalize_query_url(indicator)
            add_candidate("url", normalized_url, "inferred_url")
            add_candidate("domain", urlparse(normalized_url).netloc.lower(), "domain_from_url")
        except ValueError:
            pass
    else:
        try:
            add_candidate("domain", normalize_domain(indicator), "inferred_domain")
        except ValueError:
            pass
    if not candidates:
        normalized_url = _normalize_query_url(indicator)
        add_candidate("url", normalized_url, "fallback_url")
        add_candidate("domain", urlparse(normalized_url).netloc.lower(), "domain_from_url")
    return candidates


def _parse_request_payload(event: dict[str, Any]) -> dict[str, Any]:
    query = dict(event.get("queryStringParameters") or {})
    body = event.get("body")
    if body is None:
        return query
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    payload = json.loads(body) if body else {}
    merged = dict(query)
    merged.update(payload)
    return merged


def handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    if lookup_table is None:
        return _json_response(500, {"error": "LOOKUP_TABLE is not configured"})

    try:
        payload = _parse_request_payload(event)
    except Exception as exc:  # noqa: BLE001
        return _json_response(400, {"error": f"invalid-request: {exc}"})

    candidates = build_lookup_candidates(payload)
    if not candidates:
        return _json_response(400, {"error": "Provide one of: indicator, url, domain."})

    matches: list[dict[str, Any]] = []
    for candidate in candidates:
        response = lookup_table.get_item(Key={"indicator_key": candidate["indicator_key"]})
        item = response.get("Item")
        if not item:
            continue
        match = dict(item)
        match["matched_via"] = candidate["matched_via"]
        matches.append(match)

    metadata_response = lookup_table.get_item(Key={"indicator_key": LOOKUP_METADATA_KEY})
    index_metadata = metadata_response.get("Item")

    return _json_response(
        200,
        {
            "found": bool(matches),
            "query": payload,
            "index_metadata": index_metadata,
            "candidate_count": len(candidates),
            "candidates": candidates,
            "match_count": len(matches),
            "matches": matches,
        },
    )
