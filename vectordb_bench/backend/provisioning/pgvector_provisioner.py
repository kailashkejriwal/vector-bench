"""PgVector Docker provisioner."""

from pydantic import SecretStr

from vectordb_bench.backend.clients import DB
from vectordb_bench.backend.clients.pgvector.config import PgVectorConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo, InstanceConfig, ResourceProfile
from vectordb_bench.backend.provisioning.docker_base import DockerContainerProvisioner

# Always use latest image (pulled via --pull always in docker_base)
PGVECTOR_IMAGE = "pgvector/pgvector:latest"
PGVECTOR_PORT = 5432
DEFAULT_USER = "postgres"
DEFAULT_PASSWORD = "postgres"
DEFAULT_DB = "vectordbbench"


class PgVectorDockerProvisioner(DockerContainerProvisioner):
    """Provision PgVector via Docker (pgvector/pgvector:pg16)."""

    image = PGVECTOR_IMAGE
    container_port = PGVECTOR_PORT
    env = [
        f"POSTGRES_USER={DEFAULT_USER}",
        f"POSTGRES_PASSWORD={DEFAULT_PASSWORD}",
        f"POSTGRES_DB={DEFAULT_DB}",
    ]

    def _connection_info(self, host_port: str) -> ConnectionInfo:
        return {
            "host": self.host,
            "port": int(host_port),
            "user_name": DEFAULT_USER,
            "password": DEFAULT_PASSWORD,
            "db_name": DEFAULT_DB,
        }

    @staticmethod
    def connection_info_to_db_config(conn: ConnectionInfo) -> PgVectorConfig:
        """Build PgVectorConfig from provisioner connection info."""
        return PgVectorConfig(
            host=conn["host"],
            port=conn["port"],
            user_name=SecretStr(conn["user_name"]),
            password=SecretStr(conn["password"]),
            db_name=conn["db_name"],
        )
