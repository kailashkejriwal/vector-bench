"""Resource profiles keyed by dataset size/dimension for auto-provisioned instances."""

import logging
import re
from typing import Any

from vectordb_bench.backend.clients import DB

from .base import ResourceProfile, InstanceConfig

log = logging.getLogger(__name__)

# PgVector index build: maintenance_work_mem + parallel workers + Postgres need headroom. Docker Desktop VM RAM
# is often smaller than the container limit; 16Gi reduces OOM kills during CREATE INDEX on laptops.
_PGVECTOR_AUTO_PROVISION_MIN_MEMORY = "16Gi"
_PGVECTOR_AUTO_PROVISION_MIN_CPU = "2"

# Map (size, dim) -> default CPU, memory. Used when no custom manifest or overrides.
# Milvus querynode: steady memUsage can be high; a single sealed segment can be ~2GiB+ to load
# (predictMemUsage > memLimit at 32Gi). Use 64Gi for 10M unless you override downward.
_SIZE_DIM_TO_PROFILE: dict[tuple[int, int], tuple[str, str]] = {
    (50_000, 1536): ("1", "4Gi"),
    (100_000, 768): ("2", "8Gi"),
    (500_000, 128): ("2", "4Gi"),
    (500_000, 1536): ("2", "4Gi"),
    (500_000, 960): ("2", "4Gi"),
    (1_000_000, 768): ("4", "16Gi"),
    (1_000_000, 1024): ("4", "16Gi"),
    (5_000_000, 1536): ("4", "16Gi"),
    (10_000_000, 768): ("12", "64Gi"),
    (10_000_000, 1024): ("12", "64Gi"),
    (100_000_000, 768): ("16", "128Gi"),
}

# Fallback when (size, dim) not in map
_DEFAULT_CPU = "2"
_DEFAULT_MEMORY = "4Gi"


def _mem_to_gib(memory: str) -> float:
    """Parse K8s-style memory (e.g. 4Gi, 512Mi) to gibibytes for comparison."""
    m = re.match(r"^(\d+(?:\.\d+)?)\s*([gGmMkK]i?)?$", (memory or "").strip(), re.I)
    if not m:
        return 0.0
    n = float(m.group(1))
    u = (m.group(2) or "g").lower().replace("i", "")
    if u == "g":
        return n
    if u == "m":
        return n / 1024.0
    if u == "k":
        return n / (1024.0 * 1024.0)
    return n


def _max_memory_str(a: str, b: str) -> str:
    return a if _mem_to_gib(a) >= _mem_to_gib(b) else b


def _max_cpu_str(a: str, b: str) -> str:
    try:
        fa, fb = float(a), float(b)
    except ValueError:
        return a
    return a if fa >= fb else b


def get_resource_profile(
    data_size: int,
    dim: int,
    instance_config: InstanceConfig | None = None,
    db: DB | None = None,
) -> ResourceProfile:
    """Build a ResourceProfile from dataset size/dim and optional user overrides."""
    key = (data_size, dim)
    # Find best match: exact or smallest size >= data_size with same dim
    cpu, memory = _DEFAULT_CPU, _DEFAULT_MEMORY
    for (s, d), (c, m) in sorted(_SIZE_DIM_TO_PROFILE.items(), key=lambda x: (x[0][0], x[0][1])):
        if d == dim and s >= data_size:
            cpu, memory = c, m
            break
        elif d == dim and s < data_size:
            cpu, memory = c, m  # use last matching dim
    profile = ResourceProfile(cpu=cpu, memory=memory)
    # PgVector: parallel CREATE INDEX is memory-heavy; small --memory or --cpus OOM-kills or starves the server.
    if db == DB.PgVector:
        has_mem_override = bool(
            instance_config and instance_config.resource_overrides and "memory" in instance_config.resource_overrides
        )
        has_cpu_override = bool(
            instance_config and instance_config.resource_overrides and "cpu" in instance_config.resource_overrides
        )
        if not has_mem_override:
            before = profile.memory
            profile.memory = _max_memory_str(profile.memory, _PGVECTOR_AUTO_PROVISION_MIN_MEMORY)
            if profile.memory != before:
                log.info(
                    "PgVector auto-provision: raised docker memory %s → %s (CREATE INDEX / maintenance_work_mem headroom)",
                    before,
                    profile.memory,
                )
        if not has_cpu_override:
            before_cpu = profile.cpu
            profile.cpu = _max_cpu_str(profile.cpu, _PGVECTOR_AUTO_PROVISION_MIN_CPU)
            if profile.cpu != before_cpu:
                log.info(
                    "PgVector auto-provision: raised docker cpus %s → %s (parallel index build)",
                    before_cpu,
                    profile.cpu,
                )

    if instance_config and instance_config.resource_overrides:
        overrides = instance_config.resource_overrides
        if "cpu" in overrides:
            profile.cpu = str(overrides["cpu"])
        if "memory" in overrides:
            profile.memory = str(overrides["memory"])
        if "replicas" in overrides:
            profile.replicas = int(overrides["replicas"])
    return profile
