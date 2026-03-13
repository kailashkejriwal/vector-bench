"""Auto-provisioning of vector database instances (Docker/Colima, optional K8s)."""

from .base import (
    ConnectionInfo,
    InstanceConfig,
    Provisioner,
    ResourceProfile,
)
from .registry import (
    connection_info_to_db_config,
    get_provisioner,
    supports_auto_provision,
)

__all__ = [
    "ConnectionInfo",
    "InstanceConfig",
    "Provisioner",
    "ResourceProfile",
    "connection_info_to_db_config",
    "get_provisioner",
    "supports_auto_provision",
]
