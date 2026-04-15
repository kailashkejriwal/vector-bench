"""Remove host-mounted Docker DB data after auto-provision teardown (disk metrics baseline).

Dataset downloads (DATASET_LOCAL_DIR) are never touched.
"""

from __future__ import annotations

import logging
import pathlib
import shutil

from vectordb_bench import config
from vectordb_bench.backend.clients import DB

log = logging.getLogger(__name__)


def _resolved(p: pathlib.Path) -> pathlib.Path:
    return p.expanduser().resolve()


def _is_under_or_equal(path: pathlib.Path, ancestor: pathlib.Path) -> bool:
    try:
        path.relative_to(ancestor)
        return True
    except ValueError:
        return False


def _unsafe_clear_reason(data_dir: pathlib.Path) -> str | None:
    if not data_dir.is_absolute():
        return "path must be absolute"
    try:
        resolved = _resolved(data_dir)
    except OSError as e:
        return str(e)
    if resolved == pathlib.Path("/"):
        return "refusing filesystem root"

    dataset_root = _resolved(pathlib.Path(config.DATASET_LOCAL_DIR))
    results_root = _resolved(pathlib.Path(config.RESULTS_LOCAL_DIR))
    cfg_root = _resolved(pathlib.Path(config.CONFIG_LOCAL_DIR))

    for forbidden_name, forbidden in (
        ("DATASET_LOCAL_DIR", dataset_root),
        ("RESULTS_LOCAL_DIR", results_root),
        ("CONFIG_LOCAL_DIR", cfg_root),
    ):
        if resolved == forbidden:
            return f"matches {forbidden_name}"
        if _is_under_or_equal(resolved, forbidden):
            return f"under {forbidden_name}"
        if _is_under_or_equal(forbidden, resolved):
            return f"contains {forbidden_name}"

    return None


def _raw_dir_for_db(db: DB) -> str:
    mapping: dict[DB, str] = {
        DB.Clickhouse: config.CLICKHOUSE_DATA_DIR,
        DB.Milvus: config.MILVUS_DATA_DIR,
        DB.QdrantLocal: config.QDRANT_DATA_DIR,
        DB.PgVector: config.PGVECTOR_DATA_DIR,
    }
    return (mapping.get(db) or "").strip()


def clear_auto_provision_host_data_dir(db: DB) -> None:
    """Delete contents of the configured host data dir for this DB, then recreate an empty directory.

    No-op if PROVISION_CLEAR_HOST_DATA_AFTER_RUN is false, path unset, or safety checks fail.
    """
    if not getattr(config, "PROVISION_CLEAR_HOST_DATA_AFTER_RUN", False):
        return

    raw = _raw_dir_for_db(db)
    if not raw:
        log.debug("No host data dir configured for %s; skip post-run cleanup", db.name)
        return

    data_path = pathlib.Path(raw)
    reason = _unsafe_clear_reason(data_path)
    if reason:
        log.warning("Skip clearing host data dir for %s (%s): %s", db.name, data_path, reason)
        return

    resolved = _resolved(data_path)
    if not resolved.exists():
        log.debug("Host data dir does not exist; skip: %s", resolved)
        return

    log.info("Post-run cleanup: removing host data dir for %s → %s", db.name, resolved)
    try:
        shutil.rmtree(resolved)
        resolved.mkdir(parents=True, exist_ok=True)
        log.info("Post-run cleanup: recreated empty dir %s", resolved)
    except OSError as e:
        log.warning("Post-run cleanup failed for %s (%s): %s", db.name, resolved, e)
