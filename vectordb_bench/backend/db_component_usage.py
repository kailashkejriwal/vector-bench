"""Collect per-component storage/RAM usage from the database after a run.

Currently implemented for Qdrant (self-hosted): uses the collection memory API
(``GET /collections/{name}/memory``, Qdrant >= 1.18) which reports, per component
(vector storage, vector index, quantized vectors, payload, payload indexes,
id tracker, ...):

- disk_bytes:            total file sizes on disk
- ram_bytes:             non-evictable heap RAM (not backed by mmap)
- cached_bytes:          evictable RAM (file pages resident in OS page cache)
- expected_cache_bytes:  bytes that should ideally be cached for best performance

For older Qdrant versions it falls back to ``GET /telemetry?details_level=10``
and aggregates per-segment estimates (vectors size, payload size, total RAM/disk).

The result is stored on the Metric as a JSON string so it survives the results
file round-trip, and is rendered as a dedicated sheet in the Excel export.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import requests

from vectordb_bench.backend.clients import DB

log = logging.getLogger(__name__)

_HTTP_TIMEOUT_SEC = 30

_COMPONENT_BYTE_KEYS = ("disk_bytes", "ram_bytes", "cached_bytes", "expected_cache_bytes")


def _flatten_components(node: Any, path: list[str], out: list[dict]) -> None:
    """Recursively collect dicts that carry the per-component byte counters.

    The memory API nests components (e.g. vectors -> dense -> storage/index), so
    we record every dict containing the byte keys under its joined path.
    """
    if not isinstance(node, dict):
        return
    if any(k in node for k in _COMPONENT_BYTE_KEYS) and any(
        isinstance(node.get(k), (int, float)) for k in _COMPONENT_BYTE_KEYS
    ):
        out.append(
            {
                "component": "/".join(path) if path else "total",
                **{k: int(node.get(k, 0) or 0) for k in _COMPONENT_BYTE_KEYS},
            }
        )
    for key, child in node.items():
        if key in _COMPONENT_BYTE_KEYS:
            continue
        if isinstance(child, dict):
            _flatten_components(child, [*path, str(key)], out)
        elif isinstance(child, list):
            for i, item in enumerate(child):
                _flatten_components(item, [*path, f"{key}[{i}]"], out)


def _qdrant_memory_breakdown(base_url: str, collection_name: str) -> dict | None:
    """Per-component breakdown from Qdrant's collection memory API (>= 1.18)."""
    resp = requests.get(
        f"{base_url}/collections/{collection_name}/memory",
        timeout=_HTTP_TIMEOUT_SEC,
    )
    if resp.status_code != 200:
        log.info("Qdrant memory API not available (HTTP %s); will try telemetry", resp.status_code)
        return None
    payload = resp.json()
    result = payload.get("result", payload)
    components: list[dict] = []
    _flatten_components(result, [], components)
    if not components:
        return None
    return {
        "source": "qdrant collection memory API (/collections/{name}/memory)",
        "components": components,
    }


def _qdrant_telemetry_breakdown(base_url: str) -> dict | None:
    """Coarse estimates aggregated from segment telemetry (works on older Qdrant)."""
    resp = requests.get(
        f"{base_url}/telemetry",
        params={"details_level": 10},
        timeout=_HTTP_TIMEOUT_SEC,
    )
    if resp.status_code != 200:
        return None
    result = resp.json().get("result", {})

    totals = {"vectors_size_bytes": 0, "payloads_size_bytes": 0, "ram_usage_bytes": 0, "disk_usage_bytes": 0}

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            # SegmentInfo dicts carry these estimate fields
            if "ram_usage_bytes" in node and "disk_usage_bytes" in node:
                for k in totals:
                    v = node.get(k)
                    if isinstance(v, (int, float)):
                        totals[k] += int(v)
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for v in node:
                _walk(v)

    _walk(result)
    if not any(totals.values()):
        return None
    # Only include the fields telemetry actually measures; the Excel sheet leaves
    # missing fields blank instead of showing misleading zeros.
    return {
        "source": "qdrant telemetry API (estimated, segment-level)",
        "components": [
            {"component": "vectors (estimated)", "size_bytes": totals["vectors_size_bytes"]},
            {"component": "payloads (estimated)", "size_bytes": totals["payloads_size_bytes"]},
            {
                "component": "all segments total",
                "disk_bytes": totals["disk_usage_bytes"],
                "ram_bytes": totals["ram_usage_bytes"],
            },
        ],
    }


def collect_component_usage(db: DB, db_config_dict: dict, collection_name: str) -> str:
    """Return a JSON string describing per-component disk/RAM usage, or '' if unavailable."""
    if db not in (DB.QdrantLocal,):
        return ""
    base_url = (db_config_dict.get("url") or "").rstrip("/")
    if not base_url or not collection_name:
        return ""
    try:
        breakdown = _qdrant_memory_breakdown(base_url, collection_name)
        if breakdown is None:
            breakdown = _qdrant_telemetry_breakdown(base_url)
        if breakdown is None:
            log.warning("Could not collect Qdrant per-component usage (memory API and telemetry both unavailable)")
            return ""
        return json.dumps(breakdown)
    except Exception as e:
        log.warning("Failed to collect per-component usage for %s: %s", db.name, e)
        return ""


def apply_component_usage_sample(metric, db: DB, db_config_dict: dict, collection_name: str) -> None:
    """Store the per-component usage JSON on the metric (no-op for unsupported DBs)."""
    usage = collect_component_usage(db, db_config_dict, collection_name)
    if usage:
        metric.db_component_usage_json = usage
