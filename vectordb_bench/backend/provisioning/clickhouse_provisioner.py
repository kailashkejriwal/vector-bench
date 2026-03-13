"""ClickHouse Docker provisioner."""

import logging
import time

from pydantic import SecretStr

from vectordb_bench.backend.clients.clickhouse.config import ClickhouseConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo
from vectordb_bench.backend.provisioning.docker_base import DockerContainerProvisioner

log = logging.getLogger(__name__)

# ClickHouse needs extra time after TCP bind before native protocol handshake succeeds.
CLICKHOUSE_POST_READINESS_DELAY_SEC = 10

# Always use latest image (pulled via --pull always in docker_base)
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:latest"
# Native TCP port for clickhouse-driver (HTTP would be 8123)
CLICKHOUSE_NATIVE_PORT = 9000
DEFAULT_USER = "default"
DEFAULT_PASSWORD = "vectordbbench"
DEFAULT_DB = "default"


class ClickhouseDockerProvisioner(DockerContainerProvisioner):
    """Provision ClickHouse via Docker (clickhouse/clickhouse-server). Uses native port 9000 for clickhouse-driver."""

    image = CLICKHOUSE_IMAGE
    container_port = CLICKHOUSE_NATIVE_PORT
    env = [f"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1", f"CLICKHOUSE_PASSWORD={DEFAULT_PASSWORD}"]

    def _wait_until_ready(self, host: str, port: int, timeout_sec: int = 600) -> None:
        """Wait for TCP then extra delay so ClickHouse is ready for native protocol."""
        super()._wait_until_ready(host, port, timeout_sec)
        log.info(
            "ClickHouse: waiting %ds for server to accept native protocol handshake",
            CLICKHOUSE_POST_READINESS_DELAY_SEC,
        )
        time.sleep(CLICKHOUSE_POST_READINESS_DELAY_SEC)

    def _connection_info(self, host_port: str) -> ConnectionInfo:
        return {
            "host": self.host,
            "port": int(host_port),
            "user": DEFAULT_USER,
            "password": DEFAULT_PASSWORD,
            "db_name": DEFAULT_DB,
            "secure": False,
        }

    @staticmethod
    def connection_info_to_db_config(conn: ConnectionInfo) -> ClickhouseConfig:
        """Build ClickhouseConfig from provisioner connection info."""
        return ClickhouseConfig(
            host=conn["host"],
            port=conn["port"],
            user=conn["user"],
            password=SecretStr(conn["password"]),
            db_name=conn["db_name"],
            secure=conn["secure"],
        )
