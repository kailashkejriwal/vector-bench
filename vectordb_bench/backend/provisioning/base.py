"""Base types for auto-provisioning of vector database instances."""

from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

# Connection details returned by a provisioner (DB-specific keys)
ConnectionInfo = dict[str, Any]


class ResourceProfile(BaseModel):
    """CPU/memory (and optional overrides) for a provisioned instance."""

    cpu: str = "2"
    memory: str = "4Gi"
    replicas: int | None = None  # optional for K8s/manifests


class InstanceConfig(BaseModel):
    """User-supplied instance config: custom manifest and/or resource overrides."""

    use_custom_manifest: bool = False
    manifest_yaml: str | None = None
    manifest_format: str | None = None  # "kubernetes" | "docker_compose"
    resource_overrides: dict[str, Any] | None = None  # e.g. {"cpu": "4", "memory": "8Gi"}
    leave_container_running: bool = False  # skip teardown so you can analyze/interact with the container after the run


class Provisioner(ABC):
    """Abstract provisioner: start instance, return connection info, teardown."""

    @abstractmethod
    def provision(
        self,
        resource_profile: ResourceProfile,
        instance_config: InstanceConfig | None = None,
        context: dict[str, Any] | None = None,
    ) -> ConnectionInfo:
        """Start the database instance and return connection info."""
        raise NotImplementedError

    @abstractmethod
    def teardown(self, leave_running: bool = False) -> None:
        """Stop and remove the instance. If leave_running=True, do nothing (container stays up for analysis)."""
        raise NotImplementedError

    def is_available(self) -> bool:
        """Check if the provisioner can run (e.g. Docker available)."""
        return True
