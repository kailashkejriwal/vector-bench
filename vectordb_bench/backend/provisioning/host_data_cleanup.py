"""Remove host-mounted Docker DB data before provision and after teardown (auto-provision only).

Dataset downloads (DATASET_LOCAL_DIR) are never touched.
"""

from __future__ import annotations

import errno
import logging
import os
import pathlib
import shutil
import subprocess

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


def _permission_denied(exc: OSError) -> bool:
    if isinstance(exc, PermissionError):
        return True
    return exc.errno in (errno.EACCES, errno.EPERM)


def _chown_mount_via_docker(host_path: pathlib.Path, *, label: str) -> bool:
    """chown bind-mounted dir to bench uid/gid via a root process in docker (no host sudo needed)."""
    if not getattr(config, "PROVISION_CLEAR_HOST_DATA_DOCKER_CHOWN_FALLBACK", True):
        return False
    if os.name == "nt":
        return False
    mount = "/vectordb_bench_cleanup"
    uid = os.getuid()
    gid = os.getgid()
    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{host_path}:{mount}:rw",
        "alpine:3.20",
        "chown",
        "-R",
        f"{uid}:{gid}",
        mount,
    ]
    log.info("%s: fixing ownership via docker chown → %s", label, host_path)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except FileNotFoundError:
        log.debug("%s: docker not found for chown of %s", label, host_path)
        return False
    except subprocess.TimeoutExpired:
        log.warning("%s: docker chown timed out for %s", label, host_path)
        return False
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip()
        log.debug("%s: docker chown failed for %s: %s", label, host_path, err or r.returncode)
        return False
    return True


def _chown_mount_to_process_user(host_path: pathlib.Path, *, label: str) -> bool:
    """Recursively chown the data dir to the bench uid/gid (host chown, sudo -n, or euid 0)."""
    if not getattr(config, "PROVISION_CLEAR_HOST_DATA_SUDO_CHOWN", True):
        return False
    if os.name == "nt":
        return False

    uid = os.getuid()
    gid = os.getgid()
    hp = str(host_path)
    try:
        is_root = os.geteuid() == 0
    except AttributeError:
        is_root = False
    if is_root:
        cmd = ["chown", "-R", f"{uid}:{gid}", hp]
    else:
        log.info("%s: fixing ownership via sudo chown → %s", label, host_path)
        cmd = ["sudo", "-n", "chown", "-R", f"{uid}:{gid}", hp]

    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except FileNotFoundError:
        log.debug("%s: chown/sudo not found for %s", label, host_path)
        return False
    except subprocess.TimeoutExpired:
        log.warning("%s: chown timed out for %s", label, host_path)
        return False
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "").strip()
        log.debug("%s: sudo chown failed for %s: %s", label, host_path, err or r.returncode)
        return False
    return True


def _fix_ownership_for_rmtree(host_path: pathlib.Path, *, label: str) -> bool:
    """So non-root bench user can rmtree root-owned bind-mount data: docker chown, then sudo chown."""
    if _chown_mount_via_docker(host_path, label=label):
        return True
    if _chown_mount_to_process_user(host_path, label=label):
        return True
    log.warning(
        "%s: could not fix ownership of %s (enable docker for user or passwordless sudo for chown on DB data dirs)",
        label,
        host_path,
    )
    return False


def _raw_dir_for_db(db: DB) -> str:
    mapping: dict[DB, str] = {
        DB.Clickhouse: config.CLICKHOUSE_DATA_DIR,
        DB.Milvus: config.MILVUS_DATA_DIR,
        DB.QdrantLocal: config.QDRANT_DATA_DIR,
        DB.PgVector: config.PGVECTOR_DATA_DIR,
    }
    return (mapping.get(db) or "").strip()


def _make_container_writable(path: pathlib.Path, *, label: str) -> None:
    """Ensure bind-mount data dir is writable by non-root DB users in containers."""
    try:
        path.chmod(0o777)
    except OSError as e:
        log.warning("%s cleanup: could not set writable permissions on %s: %s", label, path, e)


def clear_auto_provision_host_data_dir(db: DB, *, phase: str = "post-run") -> None:
    """Delete the configured host data dir for this DB (if it exists), then recreate an empty directory.

    For ``phase="pre-provision"``, if the path does not exist yet, only ``mkdir`` is performed so Docker can bind-mount.

    No-op if PROVISION_CLEAR_HOST_DATA_AFTER_RUN is false, path unset, or safety checks fail.
    """
    if not getattr(config, "PROVISION_CLEAR_HOST_DATA_AFTER_RUN", False):
        return

    raw = _raw_dir_for_db(db)
    if not raw:
        log.debug("No host data dir configured for %s; skip %s cleanup", db.name, phase)
        return

    data_path = pathlib.Path(raw)
    reason = _unsafe_clear_reason(data_path)
    if reason:
        log.warning("Skip clearing host data dir for %s (%s): %s", db.name, data_path, reason)
        return

    resolved = _resolved(data_path)
    label = "Pre-provision" if phase == "pre-provision" else "Post-run"

    if not resolved.exists():
        if phase == "pre-provision":
            try:
                resolved.mkdir(parents=True, exist_ok=True)
                _make_container_writable(resolved, label=label)
                log.info("Pre-provision: created empty data dir %s", resolved)
            except OSError as e:
                log.warning("Pre-provision: could not create data dir %s: %s", resolved, e)
        else:
            log.debug("Host data dir does not exist; skip post-run cleanup: %s", resolved)
        return

    log.info("%s cleanup: removing host data dir for %s → %s", label, db.name, resolved)
    try:
        shutil.rmtree(resolved)
        resolved.mkdir(parents=True, exist_ok=True)
        _make_container_writable(resolved, label=label)
        log.info("%s cleanup: recreated empty dir %s", label, resolved)
    except OSError as e:
        if _permission_denied(e):
            if _fix_ownership_for_rmtree(resolved, label=label):
                try:
                    shutil.rmtree(resolved)
                    resolved.mkdir(parents=True, exist_ok=True)
                    _make_container_writable(resolved, label=label)
                    log.info(
                        "%s cleanup: recreated empty dir %s (after ownership fix)",
                        label,
                        resolved,
                    )
                except OSError as e2:
                    log.warning(
                        "%s cleanup failed for %s (%s) after ownership fix: %s",
                        label,
                        db.name,
                        resolved,
                        e2,
                    )
            # else: _fix_ownership_for_rmtree already logged
        else:
            log.warning("%s cleanup failed for %s (%s): %s", label, db.name, resolved, e)
