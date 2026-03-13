"""Milvus Kubernetes (kubectl) provisioner."""

from pydantic import SecretStr

from vectordb_bench.backend.clients.milvus.config import MilvusConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo
from vectordb_bench.backend.provisioning.kubectl_base import KubernetesContainerProvisioner

MILVUS_IMAGE = "milvusdb/milvus:latest"
MILVUS_PORT = 19530


class MilvusKubernetesProvisioner(KubernetesContainerProvisioner):
    """Provision Milvus standalone via Kubernetes (kubectl apply + port-forward)."""

    name = "milvus"
    image = MILVUS_IMAGE
    container_port = MILVUS_PORT
    env = ["ETCD_USE_EMBED=true", "COMMON_STORAGETYPE=local"]

    def _connection_info(self, host_port: str) -> ConnectionInfo:
        return {
            "uri": f"http://{self.host}:{host_port}",
        }

    @staticmethod
    def connection_info_to_db_config(conn: ConnectionInfo) -> MilvusConfig:
        """Build MilvusConfig from provisioner connection info."""
        return MilvusConfig(
            uri=SecretStr(conn["uri"]),
        )
