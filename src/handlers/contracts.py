from __future__ import annotations

from typing import Any, NotRequired, TypedDict


class SourceSummary(TypedDict, total=False):
    source_id: str | None
    display_name: str | None
    record_count: int | None
    normalized_key: str | None
    raw_key: str | None
    metadata_url: str | None
    license_url: str | None
    fetched_at: str | None
    status_code: int | None
    changed: bool
    short_circuit_reason: str | None


class CollectionSummary(TypedDict):
    source_count: int
    changed_count: int
    unchanged_count: int
    status_code_counts: dict[str, int]
    short_circuit_reason_counts: dict[str, int]
    sources: list[SourceSummary]


class CuratedLatestPointer(TypedDict, total=False):
    schema_version: int | None
    run_id: str | None
    generated_at: str | None
    published_at: str
    manifest_key: str
    combined_record_count: int | None
    delta_counts: dict[str, int]
    source_count: int | None


class PublishedLatestPointer(CuratedLatestPointer, total=False):
    authoritative_store: str
    s3_pointer_written: bool
    s3_pointer_write_attempts: int
    warning: str


class WorkflowLockRecord(TypedDict, total=False):
    source_id: str
    record_type: str
    owner_run_id: str
    trigger: str | None
    requested_by: str | None
    acquired_at: str
    lease_expires_at: int
    ttl: int


class LookupSyncStats(TypedDict):
    run_id: str
    generated_at: str
    lookup_table: str
    metadata_key: str
    sync_mode: str
    upserted_count: int
    new_count: int
    changed_count: int
    deleted_count: int
    last_rebuild_run_id: str | None
    manifest_key: str | None


class LookupMetadataRecord(TypedDict, total=False):
    indicator_key: str
    record_type: str
    schema_version: int
    generated_at: str
    run_id: str
    sync_mode: str
    upserted_count: int
    new_count: int
    changed_count: int
    deleted_count: int
    source_count: int
    combined_record_count: int | None
    delta_counts: dict[str, int]
    collection_summary: CollectionSummary | dict[str, Any]
    last_rebuild_run_id: str | None
    manifest_key: str | None
    warning: NotRequired[str]
