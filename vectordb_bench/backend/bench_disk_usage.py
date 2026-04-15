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


def _du_sb_subprocess(path: pathlib.Path, cmd: list[str]) -> int:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if r.returncode == 0 and r.stdout:
            return int(r.stdout.split()[0])
    except (ValueError, subprocess.TimeoutExpired, FileNotFoundError, IndexError, OSError):
        pass
    return 0


def _du_user(path: pathlib.Path) -> int:
    du_bin = os.environ.get("BENCHMARK_DISK_USAGE_DU_PATH", "").strip() or None
    candidates = [du_bin] if du_bin else ["du"]
    for du_cmd in candidates:
        if not du_cmd:
            continue
        n = _du_sb_subprocess(path, [du_cmd, "-sb", str(path)])
        if n > 0:
            return n
    return 0


def _walk_file_bytes_sum(path: pathlib.Path) -> int:
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


def _du_sudo(path: pathlib.Path) -> int:
    if not getattr(config, "PROVISION_CLEAR_HOST_DATA_SUDO_CHOWN", True):
        return 0
    if os.name == "nt":
        return 0
    try:
        is_root = os.geteuid() == 0
    except AttributeError:
        is_root = False
    cmd = ["du", "-sb", str(path)] if is_root else ["sudo", "-n", "du", "-sb", str(path)]
    return _du_sb_subprocess(path, cmd)


def _du_docker(path: pathlib.Path) -> int:
    if not getattr(config, "PROVISION_CLEAR_HOST_DATA_DOCKER_CHOWN_FALLBACK", True):
        return 0
    if os.name == "nt":
        return 0
    mount = "/_vdb_sz"
    try:
        resolved = path.resolve()
        r = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{resolved}:{mount}:ro",
                "alpine:3.20",
                "du",
                "-sk",
                mount,
            ],
            capture_output=True,
            text=True,
            timeout=600,
        )
        if r.returncode == 0 and r.stdout.strip():
            kb = int(r.stdout.split()[0])
            return kb * 1024
    except (ValueError, subprocess.TimeoutExpired, FileNotFoundError, IndexError, OSError) as e:
        log.debug("docker du failed for %s: %s", path, e)
    return 0


def _elevated_du(path: pathlib.Path) -> int:
    n = _du_sudo(path)
    if n > 0:
        return n
    return _du_docker(path)


def directory_size_bytes(path: pathlib.Path) -> int:
    """Recursive size under ``path``. Uses elevated du (sudo/docker) when the bench user cannot read the tree."""
    if not path.exists():
        return 0

    try:
        first_entry = next(path.iterdir(), None)
    except PermissionError:
        log.debug("disk usage: cannot list %s as bench user; trying sudo/docker du", path)
        return _elevated_du(path)
    except OSError as e:
        log.debug("disk usage: listdir %s: %s; trying elevated du", path, e)
        return _elevated_du(path)

    if first_entry is None:
        return 0

    n = _du_user(path)
    if n > 0:
        return n
    n = _walk_file_bytes_sum(path)
    if n > 0:
        return n

    log.debug("disk usage: user du/walk returned 0 for non-empty %s; trying elevated du", path)
    return _elevated_du(path)


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
