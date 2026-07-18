"""Container-scoped resource monitoring via `docker stats`.

Unlike :class:`vectordb_bench.backend.utils.ResourceMonitor` (which samples the
whole host with psutil), this monitor reports CPU / memory / block-IO for a
single Docker container, so metrics reflect only the target database process
(e.g. Qdrant) rather than the entire VM.
"""

from __future__ import annotations

import logging
import re
import subprocess
import threading
import time
from typing import Any

from vectordb_bench import config
from vectordb_bench.backend.clients import DB

log = logging.getLogger(__name__)

# Docker image (ancestor) used to auto-detect the running container per DB.
_DB_IMAGE_ANCESTORS: dict[DB, str] = {
    DB.QdrantLocal: "qdrant/qdrant",
}


def _explicit_container_for_db(db: DB) -> str:
    if db == DB.QdrantLocal:
        return (getattr(config, "QDRANT_CONTAINER", "") or "").strip()
    return ""


def _run_docker(args: list[str], timeout: int = 15) -> str | None:
    """Run a docker CLI command; return stdout (stripped) or None on any failure."""
    try:
        proc = subprocess.run(
            ["docker", *args],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
        log.debug("docker %s failed: %s", " ".join(args), e)
        return None
    if proc.returncode != 0:
        log.debug("docker %s rc=%s stderr=%s", " ".join(args), proc.returncode, (proc.stderr or "").strip())
        return None
    return (proc.stdout or "").strip()


def _first_running_container_id(*, ancestor: str | None = None, ref: str | None = None) -> str | None:
    """Return the id of the first running container matching an ancestor image or a name/id ref."""
    args = ["ps", "--format", "{{.ID}}"]
    if ancestor:
        args += ["--filter", f"ancestor={ancestor}"]
    if ref:
        # ref may be a name or id; try both filters (docker OR-matches repeated same-key filters).
        args += ["--filter", f"name={ref}", "--filter", f"id={ref}"]
    out = _run_docker(args)
    if not out:
        return None
    ids = [line.strip() for line in out.splitlines() if line.strip()]
    return ids[0] if ids else None


def resolve_db_container_id(db: DB) -> str | None:
    """Resolve the Docker container id to monitor for this DB.

    Order: explicit config (name/id) -> auto-detect by image ancestor. Returns None if
    docker is unavailable or no matching running container is found.
    """
    explicit = _explicit_container_for_db(db)
    if explicit:
        cid = _first_running_container_id(ref=explicit)
        if cid:
            return cid
        log.warning("Configured container %r for %s not found/running; falling back to auto-detect", explicit, db.name)

    ancestor = _DB_IMAGE_ANCESTORS.get(db)
    if not ancestor:
        return None
    cid = _first_running_container_id(ancestor=ancestor)
    if not cid:
        log.warning("No running container found for image ancestor %r (%s)", ancestor, db.name)
    return cid


_SIZE_UNITS = {
    "b": 1,
    "kb": 1000, "mb": 1000**2, "gb": 1000**3, "tb": 1000**4, "pb": 1000**5,
    "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4, "pib": 1024**5,
}


def _parse_size_to_bytes(token: str) -> float:
    """Parse a docker size token like '1.5GiB', '908MB', '0B' to bytes."""
    if not token:
        return 0.0
    m = re.match(r"^\s*([\d.]+)\s*([A-Za-z]+)?\s*$", token)
    if not m:
        return 0.0
    value = float(m.group(1))
    unit = (m.group(2) or "b").lower()
    return value * _SIZE_UNITS.get(unit, 1)


def _parse_cpu_percent(token: str) -> float:
    try:
        return float((token or "").replace("%", "").strip() or 0.0)
    except ValueError:
        return 0.0


def _parse_pair(token: str) -> tuple[float, float]:
    """Parse a docker 'X / Y' pair (e.g. BlockIO '100MB / 200MB') into (X_bytes, Y_bytes)."""
    if not token or "/" not in token:
        return 0.0, 0.0
    left, right = token.split("/", 1)
    return _parse_size_to_bytes(left), _parse_size_to_bytes(right)


# Raw cgroup memory counter files, in probing order. These report memory.current /
# usage_in_bytes with NO inactive_file subtraction, so they include page cache
# (i.e. mmap-backed data like Qdrant vector storage) unlike `docker stats` MemUsage.
_CGROUP_TOTAL_MEM_FILES = (
    "/sys/fs/cgroup/memory.current",  # cgroup v2 (unified hierarchy)
    "/sys/fs/cgroup/memory/memory.usage_in_bytes",  # cgroup v1
)


def _read_container_total_memory_bytes(container_id: str, known_good_file: str | None) -> tuple[float | None, str | None]:
    """Read raw (cache-inclusive) resident memory for a container via its own cgroup file.

    Uses `docker exec` so it works regardless of the host's own cgroup layout (the container
    sees its own cgroup subtree under /sys/fs/cgroup thanks to cgroup namespacing in modern
    Docker). Returns (bytes, file_used) on success; (None, None) if neither file is readable
    (e.g. exec disabled, older Docker without cgroupns, or container just stopped).
    """
    candidates = [known_good_file] if known_good_file else list(_CGROUP_TOTAL_MEM_FILES)
    for path in candidates:
        if not path:
            continue
        out = _run_docker(["exec", container_id, "cat", path], timeout=10)
        if out is None:
            continue
        try:
            return float(out.strip().splitlines()[0]), path
        except (ValueError, IndexError):
            continue
    return None, None


class ContainerResourceMonitor:
    """Monitor CPU / memory / block-IO of a single Docker container via `docker stats`.

    Exposes the same interface and return keys as
    :class:`vectordb_bench.backend.utils.ResourceMonitor` so it is a drop-in replacement.

    Notes:
        - CPU usage is the raw `docker stats` percentage: it is relative to a single core,
          so a container using 4 full cores reports ~400%.
        - Memory usage (avg/peak_memory_usage) is docker stats MemUsage = cgroup memory.current
          - inactive_file. It approximates RSS (heap/anon memory) and EXCLUDES most file-backed
          page cache. DBs that memory-map their storage (e.g. Qdrant dense vectors, even with
          on_disk=false) keep that data in page cache, so it does NOT show up here.
        - Memory usage total (avg/peak_memory_usage_total) is the raw cgroup memory.current
          (no inactive_file subtraction), read directly from the container's own cgroup file via
          `docker exec`. This INCLUDES page cache, so it reflects the true resident footprint of
          mmap-backed data. Falls back to 0 (same as the excluding metric) if `docker exec`/cgroup
          files aren't readable (e.g. exec disabled).
        - disk_read_bytes / disk_write_bytes are the block-IO delta over the run.
    """

    #: interval between samples (seconds); each `docker stats --no-stream` call itself takes ~1-2s.
    SAMPLE_INTERVAL_SEC = 1.0

    def __init__(self, container_id: str):
        self.container_id = container_id
        self.cpu_usages: list[float] = []
        self.memory_usages: list[float] = []  # bytes, excludes page cache
        self.memory_usages_total: list[float] = []  # bytes, includes page cache
        self._cgroup_mem_file: str | None = None  # remembered once found, to avoid re-probing
        self._blkio_first: tuple[float, float] | None = None
        self._blkio_last: tuple[float, float] | None = None
        self.monitoring = False
        self.thread: threading.Thread | None = None

    def _sample(self) -> None:
        out = _run_docker(
            [
                "stats",
                "--no-stream",
                "--format",
                "{{.CPUPerc}}|{{.MemUsage}}|{{.BlockIO}}",
                self.container_id,
            ],
            timeout=20,
        )
        if not out:
            return
        # If multiple lines (shouldn't happen for a single container), take the first.
        line = out.splitlines()[0]
        parts = line.split("|")
        if len(parts) < 3:
            return
        self.cpu_usages.append(_parse_cpu_percent(parts[0]))
        mem_used, _mem_limit = _parse_pair(parts[1])
        self.memory_usages.append(mem_used)

        total_mem, found_file = _read_container_total_memory_bytes(self.container_id, self._cgroup_mem_file)
        if found_file:
            self._cgroup_mem_file = found_file
        if total_mem is not None:
            self.memory_usages_total.append(total_mem)

        blk = _parse_pair(parts[2])
        if self._blkio_first is None:
            self._blkio_first = blk
        self._blkio_last = blk

    def start_monitoring(self) -> None:
        if self.monitoring:
            return
        self.monitoring = True
        self.cpu_usages = []
        self.memory_usages = []
        self.memory_usages_total = []
        self._blkio_first = None
        self._blkio_last = None
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()

    def _monitor_loop(self) -> None:
        while self.monitoring:
            try:
                self._sample()
            except Exception:
                pass
            time.sleep(self.SAMPLE_INTERVAL_SEC)

    def stop_monitoring(self) -> dict[str, Any]:
        if not self.monitoring:
            return {}
        self.monitoring = False
        if self.thread:
            self.thread.join(timeout=25.0)

        avg_cpu = sum(self.cpu_usages) / len(self.cpu_usages) if self.cpu_usages else 0.0
        peak_cpu = max(self.cpu_usages) if self.cpu_usages else 0.0
        avg_mem = sum(self.memory_usages) / len(self.memory_usages) if self.memory_usages else 0.0
        peak_mem = max(self.memory_usages) if self.memory_usages else 0.0
        # Falls back to the anon-only figures if the cgroup file was never readable, so the
        # "total" metric is never silently lower than the "excluding cache" one it complements.
        avg_mem_total = (
            sum(self.memory_usages_total) / len(self.memory_usages_total) if self.memory_usages_total else avg_mem
        )
        peak_mem_total = max(self.memory_usages_total) if self.memory_usages_total else peak_mem

        disk_read = disk_write = 0
        if self._blkio_first and self._blkio_last:
            disk_read = int(max(0.0, self._blkio_last[0] - self._blkio_first[0]))
            disk_write = int(max(0.0, self._blkio_last[1] - self._blkio_first[1]))

        return {
            "avg_cpu_usage": avg_cpu,
            "peak_cpu_usage": peak_cpu,
            "avg_memory_usage": avg_mem / (1024 * 1024),  # MB, excludes page cache
            "peak_memory_usage": peak_mem / (1024 * 1024),  # MB, excludes page cache
            "avg_memory_usage_total": avg_mem_total / (1024 * 1024),  # MB, includes page cache
            "peak_memory_usage_total": peak_mem_total / (1024 * 1024),  # MB, includes page cache
            "disk_read_bytes": disk_read,
            "disk_write_bytes": disk_write,
        }


def make_resource_monitor(db: DB):
    """Return a monitor scoped to the DB's container when possible, else the host monitor.

    Falls back to the whole-host :class:`ResourceMonitor` (with a warning) when container
    stats are disabled, docker is unavailable, or the container can't be resolved.
    """
    from vectordb_bench.backend.utils import ResourceMonitor

    if not getattr(config, "MONITOR_DB_CONTAINER_STATS", True):
        return ResourceMonitor()

    if db not in _DB_IMAGE_ANCESTORS and not _explicit_container_for_db(db):
        return ResourceMonitor()

    cid = resolve_db_container_id(db)
    if cid:
        log.info("Resource monitoring scoped to %s container %s (container-only stats)", db.name, cid[:12])
        return ContainerResourceMonitor(cid)

    log.warning(
        "Could not resolve a container for %s; falling back to whole-host resource stats. "
        "Set QDRANT_CONTAINER or ensure the container is running for container-only metrics.",
        db.name,
    )
    return ResourceMonitor()
