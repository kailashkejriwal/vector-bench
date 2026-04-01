"""Resource profiles keyed by dataset size/dimension for auto-provisioned instances."""

import logging
from typing import Any

from .base import ResourceProfile, InstanceConfig

log = logging.getLogger(__name__)

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


def get_resource_profile(
    data_size: int,
    dim: int,
    instance_config: InstanceConfig | None = None,
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

    if instance_config and instance_config.resource_overrides:
        overrides = instance_config.resource_overrides
        if "cpu" in overrides:
            profile.cpu = str(overrides["cpu"])
        if "memory" in overrides:
            profile.memory = str(overrides["memory"])
        if "replicas" in overrides:
            profile.replicas = int(overrides["replicas"])
    return profile
