"""Milvus Docker provisioner."""

import logging
import pathlib
import time

from pydantic import SecretStr

from vectordb_bench import config
from vectordb_bench.backend.clients.milvus.config import MilvusConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo
from vectordb_bench.backend.provisioning.docker_base import DockerContainerProvisioner

log = logging.getLogger(__name__)

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

    def _wait_until_ready(self, host: str, port: int, timeout_sec: int = 600) -> None:
        super()._wait_until_ready(host, port, timeout_sec)
        extra = int(getattr(config, "MILVUS_EXTRA_READINESS_WAIT_SEC", 0) or 0)
        if extra > 0:
            log.info(
                "Milvus provisioner: waiting %ss after TCP bind for querynode / coordinator (avoid LoadCollection race)",
                extra,
            )
            time.sleep(extra)

    def _get_extra_container_args(self) -> list[str]:
        """Mount MILVUS_DATA_DIR to /var/lib/milvus when set (e.g. NVMe disk)."""
        data_dir = (config.MILVUS_DATA_DIR or "").strip()
        if not data_dir:
            return []
        path = pathlib.Path(data_dir)
        path.mkdir(parents=True, exist_ok=True)
        log.info("Milvus: using data dir on host %s (NVMe/large disk)", path)
        return ["-v", f"{path}:/var/lib/milvus"]

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
