"""Registry of provisioners per database type."""

import logging
import os
from typing import Callable

from vectordb_bench.backend.clients import DB
from vectordb_bench.backend.provisioning.base import ConnectionInfo, Provisioner

from .clickhouse_provisioner import ClickhouseDockerProvisioner
from .milvus_provisioner import MilvusDockerProvisioner
from .milvus_kubectl_provisioner import MilvusKubernetesProvisioner
from .pgvector_provisioner import PgVectorDockerProvisioner
from .qdrant_provisioner import QdrantDockerProvisioner

log = logging.getLogger(__name__)

# DB -> (Provisioner class, function to build DBConfig from ConnectionInfo)
_PROVISIONER_REGISTRY: dict[DB, tuple[type[Provisioner], Callable[[ConnectionInfo], object]]] = {
    DB.PgVector: (PgVectorDockerProvisioner, PgVectorDockerProvisioner.connection_info_to_db_config),
    DB.Milvus: (MilvusDockerProvisioner, MilvusDockerProvisioner.connection_info_to_db_config),
    DB.Clickhouse: (ClickhouseDockerProvisioner, ClickhouseDockerProvisioner.connection_info_to_db_config),
    DB.QdrantLocal: (QdrantDockerProvisioner, QdrantDockerProvisioner.connection_info_to_db_config),
}

# Kubernetes provisioners (when VECTORDB_PROVISION_BACKEND=kubectl)
_KUBERNETES_PROVISIONER_REGISTRY: dict[DB, tuple[type[Provisioner], Callable[[ConnectionInfo], object]]] = {
    DB.Milvus: (MilvusKubernetesProvisioner, MilvusKubernetesProvisioner.connection_info_to_db_config),
}


def _backend() -> str:
    return os.environ.get("VECTORDB_PROVISION_BACKEND", "docker").strip().lower()


def get_provisioner(db: DB, backend: str | None = None) -> Provisioner | None:
    """Return a new provisioner instance for the given DB and backend.

    backend: "docker" | "kubectl". If None, uses env VECTORDB_PROVISION_BACKEND (default docker).
    """
    back = (backend or _backend())
    if back == "kubectl":
        entry = _KUBERNETES_PROVISIONER_REGISTRY.get(db)
    else:
        entry = _PROVISIONER_REGISTRY.get(db)
    if not entry:
        return None
    cls, _ = entry
    return cls()


def connection_info_to_db_config(db: DB, conn: ConnectionInfo, backend: str | None = None) -> object:
    """Build the DB's config class from provisioner connection info."""
    back = backend or _backend()
    if back == "kubectl":
        entry = _KUBERNETES_PROVISIONER_REGISTRY.get(db)
    else:
        entry = _PROVISIONER_REGISTRY.get(db)
    if not entry:
        raise ValueError(f"No provisioner registered for {db} (backend={back})")
    _, builder = entry
    return builder(conn)


def supports_auto_provision(db: DB, backend: str | None = None) -> bool:
    """Return True if this DB can be auto-provisioned with the given backend.

    backend: "docker" | "kubectl". If None, returns True if any backend is supported.
    """
    if backend is None:
        return (
            db in _PROVISIONER_REGISTRY
            or db in _KUBERNETES_PROVISIONER_REGISTRY
        )
    if backend == "kubectl":
        return db in _KUBERNETES_PROVISIONER_REGISTRY
    return db in _PROVISIONER_REGISTRY
