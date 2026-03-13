"""Qdrant Docker provisioner."""

from pydantic import SecretStr

from vectordb_bench.backend.clients.qdrant_local.config import QdrantLocalConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo
from vectordb_bench.backend.provisioning.docker_base import DockerContainerProvisioner

# Official Qdrant image; port 6333 = HTTP REST API (used by qdrant-client)
QDRANT_IMAGE = "qdrant/qdrant:latest"
QDRANT_HTTP_PORT = 6333


class QdrantDockerProvisioner(DockerContainerProvisioner):
    """Provision Qdrant via Docker (qdrant/qdrant). Exposes HTTP API on 6333."""

    image = QDRANT_IMAGE
    container_port = QDRANT_HTTP_PORT

    def _connection_info(self, host_port: str) -> ConnectionInfo:
        return {
            "url": f"http://{self.host}:{host_port}",
        }

    @staticmethod
    def connection_info_to_db_config(conn: ConnectionInfo) -> QdrantLocalConfig:
        """Build QdrantLocalConfig from provisioner connection info."""
        return QdrantLocalConfig(url=SecretStr(conn["url"]))
