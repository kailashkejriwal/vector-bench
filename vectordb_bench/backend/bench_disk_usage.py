"""Measure on-disk usage of the host database data directory only (Docker bind mount path from env)."""

from __future__ import annotations

import logging
import os
import pathlib
import subprocess

from vectordb_bench import config
from vectordb_bench.backend.clients import DB

log = logging.getLogger(__name__)


def configured_host_db_data_dir(db: DB) -> pathlib.Path | None:
    """Host path for Docker/auto-provision data for this DB, if configured."""
    mapping: dict[DB, str] = {
        DB.Clickhouse: config.CLICKHOUSE_DATA_DIR,
        DB.Milvus: config.MILVUS_DATA_DIR,
        DB.QdrantLocal: config.QDRANT_DATA_DIR,
        DB.PgVector: config.PGVECTOR_DATA_DIR,
    }
    raw = (mapping.get(db) or "").strip()
    if not raw:
        return None
    p = pathlib.Path(raw).expanduser()
    if not p.is_absolute():
        return None
    try:
        return p.resolve()
    except OSError:
        return None


def directory_size_bytes(path: pathlib.Path) -> int:
    """Recursive size under ``path`` (sum of file lengths; dirs/metadata approximated by du when available)."""
    if not path.exists():
        return 0
    du_bin = os.environ.get("BENCHMARK_DISK_USAGE_DU_PATH", "").strip() or None
    candidates = [du_bin] if du_bin else ["du"]
    for du_cmd in candidates:
        if not du_cmd:
            continue
        try:
            r = subprocess.run(
                [du_cmd, "-sb", str(path)],
                capture_output=True,
                text=True,
                timeout=600,
            )
            if r.returncode == 0 and r.stdout:
                return int(r.stdout.split()[0])
        except (ValueError, subprocess.TimeoutExpired, FileNotFoundError, IndexError, OSError):
            continue
    total = 0
    try:
        for root, _dirs, files in os.walk(path, followlinks=False):
            for name in files:
                fp = pathlib.Path(root) / name
                try:
                    total += fp.stat(follow_symlinks=False).st_size
                except OSError:
                    pass
    except OSError as e:
        log.debug("directory_size_bytes walk failed for %s: %s", path, e)
    return total


def apply_disk_usage_sample(metric, db: DB, *, phase: str) -> None:
    """Record recursive bytes under this DB's host data dir only (CLICKHOUSE_DATA_DIR, etc.)."""
    dp = configured_host_db_data_dir(db)
    if not dp:
        return
    metric.bench_db_host_data_dir_path = str(dp)
    sz = directory_size_bytes(dp) if dp.exists() else 0
    if phase == "begin":
        metric.bench_db_host_data_dir_bytes_begin = sz
    else:
        metric.bench_db_host_data_dir_bytes_end = sz
        metric.bench_db_host_data_dir_bytes_written = max(
            0, metric.bench_db_host_data_dir_bytes_end - metric.bench_db_host_data_dir_bytes_begin
        )
