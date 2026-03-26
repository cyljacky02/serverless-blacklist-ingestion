from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import logging
import os
import re
from datetime import UTC, datetime
from typing import Any, Iterable, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import boto3
from botocore.exceptions import ClientError


ARTIFACTS_BUCKET = os.environ["ARTIFACTS_BUCKET"]
STATE_TABLE = os.environ["STATE_TABLE"]
LOOKUP_TABLE = os.environ.get("LOOKUP_TABLE", "").strip()
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw")
NORMALIZED_PREFIX = os.environ.get("NORMALIZED_PREFIX", "normalized")
CURATED_PREFIX = os.environ.get("CURATED_PREFIX", "curated")
DEFAULT_USER_AGENT = os.environ.get("DEFAULT_USER_AGENT", "blacklist-ingestion/0.1")
PHISHTANK_APP_KEY = os.environ.get("PHISHTANK_APP_KEY", "").strip()
SKIP_SOURCE_HOSTS = {
    host.strip().lower()
    for host in os.environ.get("SKIP_SOURCE_HOSTS", "").split(",")
    if host.strip()
}
CURATED_SCHEMA_VERSION = int(os.environ.get("CURATED_SCHEMA_VERSION", "2"))
LOOKUP_SCHEMA_VERSION = int(os.environ.get("LOOKUP_SCHEMA_VERSION", str(CURATED_SCHEMA_VERSION)))
MERGE_PARTITION_COUNT = int(os.environ.get("MERGE_PARTITION_COUNT", "16"))
INGESTION_LOCK_LEASE_SECONDS = int(os.environ.get("INGESTION_LOCK_LEASE_SECONDS", "900"))

INGESTION_LOCK_KEY = "__lock__#ingestion"
CURATED_LATEST_META_KEY = "__meta__#curated_latest"
LOOKUP_METADATA_KEY = "__meta__#lookup_index"
LOOKUP_KEY_RE = re.compile(r"^[0-9a-f]{64}$")

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
state_table = dynamodb.Table(STATE_TABLE)
lookup_table = dynamodb.Table(LOOKUP_TABLE) if LOOKUP_TABLE else None


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_epoch_seconds() -> int:
    return int(datetime.now(UTC).timestamp())


def safe_run_id(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")
    return sanitized or "run"


def http_fetch(
    url: str,
    *,
    timeout_seconds: int = 120,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], bytes, str]:
    request = Request(url, headers=headers or {}, method="GET")
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            response_headers = {key: value for key, value in response.headers.items()}
            return response.status, response_headers, response.read(), response.geturl()
    except HTTPError as exc:
        response_headers = {key: value for key, value in exc.headers.items()}
        return exc.code, response_headers, exc.read(), exc.geturl()
    except URLError as exc:
        raise RuntimeError(f"Failed to fetch {url}: {exc}") from exc


def put_object(
    key: str,
    body: bytes,
    *,
    content_type: str,
    content_encoding: str | None = None,
    metadata: dict[str, str] | None = None,
) -> None:
    request: dict[str, Any] = {
        "Bucket": ARTIFACTS_BUCKET,
        "Key": key,
        "Body": body,
        "ContentType": content_type,
        "Metadata": metadata or {},
    }
    if content_encoding:
        request["ContentEncoding"] = content_encoding
    s3_client.put_object(**request)


def get_object_bytes(key: str) -> bytes:
    response = s3_client.get_object(Bucket=ARTIFACTS_BUCKET, Key=key)
    return response["Body"].read()


def try_get_object_bytes(key: str) -> bytes | None:
    try:
        return get_object_bytes(key)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise


def load_json_object(key: str) -> dict[str, Any] | None:
    payload = try_get_object_bytes(key)
    if payload is None:
        return None
    return json.loads(payload.decode("utf-8"))


def iter_gzip_jsonl(key: str) -> Iterator[dict[str, Any]]:
    response = s3_client.get_object(Bucket=ARTIFACTS_BUCKET, Key=key)
    body = response["Body"]
    try:
        with gzip.GzipFile(fileobj=body, mode="rb") as gzip_stream:
            with io.TextIOWrapper(gzip_stream, encoding="utf-8") as text_stream:
                for line in text_stream:
                    line = line.strip()
                    if not line:
                        continue
                    yield json.loads(line)
    finally:
        body.close()


def log_json(logger: logging.Logger, payload: dict[str, Any], *, warning: bool = False) -> None:
    level = logging.WARNING if warning else logging.INFO
    logger.log(level, json.dumps(payload, ensure_ascii=True, sort_keys=True))


def load_source_state(source_id: str, *, consistent_read: bool = False) -> dict[str, Any]:
    from state_store import load_source_state as _load_source_state

    return _load_source_state(source_id, consistent_read=consistent_read)


def save_source_state(source_id: str, attributes: dict[str, Any]) -> None:
    from state_store import save_source_state as _save_source_state

    _save_source_state(source_id, attributes)


def load_latest_curated_metadata(*, consistent_read: bool = True) -> dict[str, Any]:
    from state_store import load_latest_curated_metadata as _load_latest_curated_metadata

    return _load_latest_curated_metadata(consistent_read=consistent_read)


def source_raw_key(source_id: str, run_id: str, extension: str) -> str:
    return f"{RAW_PREFIX}/{source_id}/{run_id}/payload{extension}"


def source_normalized_key(source_id: str, run_id: str) -> str:
    return f"{NORMALIZED_PREFIX}/{source_id}/{run_id}/records.jsonl.gz"


def curated_key(name: str) -> str:
    return f"{CURATED_PREFIX}/latest/{name}"


def curated_run_key(run_id: str, name: str) -> str:
    return f"{CURATED_PREFIX}/runs/{safe_run_id(run_id)}/{name}"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def indicator_identity(indicator_type: str, indicator: str) -> str:
    return f"{indicator_type}#{indicator}"


def indicator_lookup_key(indicator_type: str, indicator: str) -> str:
    return sha256_hex(indicator_identity(indicator_type, indicator).encode("utf-8"))


def indicator_partition_id(indicator_type: str, indicator: str, *, partition_count: int = MERGE_PARTITION_COUNT) -> str:
    if partition_count <= 0:
        raise ValueError("partition_count must be positive")
    bucket = int(indicator_lookup_key(indicator_type, indicator)[:8], 16) % partition_count
    return f"{bucket:02x}"


def partition_ids(*, partition_count: int = MERGE_PARTITION_COUNT) -> list[str]:
    return [f"{bucket:02x}" for bucket in range(partition_count)]


def is_hashed_lookup_key(value: str) -> bool:
    return bool(LOOKUP_KEY_RE.fullmatch(value))


def json_dumps(data: Any) -> bytes:
    return json.dumps(data, ensure_ascii=True, sort_keys=True, indent=2).encode("utf-8")


def gzip_jsonl(records: Iterable[dict[str, Any]]) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=True, sort_keys=True).encode("utf-8"))
            handle.write(b"\n")
    return buffer.getvalue()


def read_gzip_jsonl(data: bytes) -> list[dict[str, Any]]:
    with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as handle:
        content = handle.read().decode("utf-8")
    records: list[dict[str, Any]] = []
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
    return records


def csv_rows_from_bytes(data: bytes, *, encoding: str = "utf-8-sig") -> list[dict[str, str]]:
    text = data.decode(encoding)
    return list(csv.DictReader(io.StringIO(text)))


def normalize_url(value: str) -> str:
    parsed = urlparse(value.strip())
    if not parsed.scheme:
        raise ValueError(f"URL is missing scheme: {value}")
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path or "/"
    query = parsed.query
    return f"{scheme}://{netloc}{path}" + (f"?{query}" if query else "")


def normalize_domain(value: str) -> str:
    domain = value.strip().lower().rstrip(".")
    if not domain:
        raise ValueError("Domain is empty")
    if any(token in domain for token in ("/", ":", "?", "#")):
        raise ValueError(f"Domain contains URL delimiters: {value}")
    return domain


def build_common_headers(existing_state: dict[str, Any] | None = None) -> dict[str, str]:
    headers = {"User-Agent": DEFAULT_USER_AGENT}
    if existing_state:
        etag = existing_state.get("etag")
        last_modified = existing_state.get("last_modified")
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
    return headers
