"""Milvus Docker provisioner."""

from pydantic import SecretStr

from vectordb_bench.backend.clients.milvus.config import MilvusConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo
from vectordb_bench.backend.provisioning.docker_base import DockerContainerProvisioner

# Always use latest image (pulled via --pull always in docker_base)
MILVUS_IMAGE = "milvusdb/milvus:latest"
MILVUS_PORT = 19530


class MilvusDockerProvisioner(DockerContainerProvisioner):
    """Provision Milvus standalone via Docker."""

    image = MILVUS_IMAGE
    container_port = MILVUS_PORT
    env = ["ETCD_USE_EMBED=true", "COMMON_STORAGETYPE=local", "DEPLOY_MODE=STANDALONE"]
    # Image entrypoint is tini; must pass the actual server command (see milvus.io/docs install_standalone-docker)
    command = ["milvus", "run", "standalone"]

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
