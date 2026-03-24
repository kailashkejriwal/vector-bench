"""Qdrant Docker provisioner."""

import logging
import pathlib

from pydantic import SecretStr

from vectordb_bench import config
from vectordb_bench.backend.clients.qdrant_local.config import QdrantLocalConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo
from vectordb_bench.backend.provisioning.docker_base import DockerContainerProvisioner

log = logging.getLogger(__name__)

# Official Qdrant image; port 6333 = HTTP REST API (used by qdrant-client)
QDRANT_IMAGE = "qdrant/qdrant:latest"
QDRANT_HTTP_PORT = 6333
# Default persistence path inside the official image (see qdrant.io quickstart)
QDRANT_CONTAINER_STORAGE = "/qdrant/storage"


class QdrantDockerProvisioner(DockerContainerProvisioner):
    """Provision Qdrant via Docker (qdrant/qdrant). Exposes HTTP API on 6333."""

    image = QDRANT_IMAGE
    container_port = QDRANT_HTTP_PORT

    def _get_extra_container_args(self) -> list[str]:
        """Mount QDRANT_DATA_DIR to /qdrant/storage when set (e.g. NVMe disk)."""
        data_dir = (config.QDRANT_DATA_DIR or "").strip()
        if not data_dir:
            return []
        path = pathlib.Path(data_dir)
        path.mkdir(parents=True, exist_ok=True)
        log.info("Qdrant: using storage dir on host %s (NVMe/large disk)", path)
        return ["-v", f"{path}:{QDRANT_CONTAINER_STORAGE}"]

    def _connection_info(self, host_port: str) -> ConnectionInfo:
        return {
            "url": f"http://{self.host}:{host_port}",
        }

    @staticmethod
    def connection_info_to_db_config(conn: ConnectionInfo) -> QdrantLocalConfig:
        """Build QdrantLocalConfig from provisioner connection info."""
        return QdrantLocalConfig(url=SecretStr(conn["url"]))
