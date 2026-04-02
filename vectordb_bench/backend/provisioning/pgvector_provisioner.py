"""PgVector Docker provisioner."""

import logging
import pathlib
import time

from pydantic import SecretStr

from vectordb_bench import config
from vectordb_bench.backend.clients.pgvector.config import PgVectorConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo
from vectordb_bench.backend.provisioning.docker_base import (
    DockerContainerProvisioner,
    _memory_for_docker,
)

log = logging.getLogger(__name__)

PGVECTOR_PORT = 5432
DEFAULT_USER = "postgres"
DEFAULT_PASSWORD = "postgres"
DEFAULT_DB = "vectordbbench"
# Extra wait after TCP accept: cold data dir init / restart can lag behind port open.
PGVECTOR_POST_READINESS_DELAY_SEC = 5
# Official Postgres image PGDATA
PGVECTOR_CONTAINER_PGDATA = "/var/lib/postgresql/data"


class PgVectorDockerProvisioner(DockerContainerProvisioner):
    """Provision PgVector via Docker (pgvector/pgvector, Postgres + extension)."""

    container_port = PGVECTOR_PORT
    env = [
        f"POSTGRES_USER={DEFAULT_USER}",
        f"POSTGRES_PASSWORD={DEFAULT_PASSWORD}",
        f"POSTGRES_DB={DEFAULT_DB}",
    ]

    def __init__(self) -> None:
        # Pin via PGVECTOR_DOCKER_IMAGE — :latest is often missing on Docker Hub.
        img = (config.PGVECTOR_DOCKER_IMAGE or "").strip()
        self.image = img or "pgvector/pgvector:pg16"

    def _docker_shm_size_args(self) -> list[str]:
        raw = (getattr(config, "PGVECTOR_DOCKER_SHM_SIZE", "") or "").strip()
        if not raw or raw in {"0", "none", "off"}:
            return []
        size = _memory_for_docker(raw)
        log.info(
            "PgVector Docker: --shm-size=%s (parallel index builds need /dev/shm; Docker default 64m is too small)",
            size,
        )
        return ["--shm-size", size]

    def _get_extra_container_args(self) -> list[str]:
        """Mount PGVECTOR_DATA_DIR to Postgres PGDATA when set (NVMe / persistence)."""
        data_dir = (config.PGVECTOR_DATA_DIR or "").strip()
        if not data_dir:
            return []
        path = pathlib.Path(data_dir)
        path.mkdir(parents=True, exist_ok=True)
        log.info("PgVector: mounting host data dir %s → %s", path, PGVECTOR_CONTAINER_PGDATA)
        return ["-v", f"{path}:{PGVECTOR_CONTAINER_PGDATA}"]

    def _wait_until_ready(self, host: str, port: int, timeout_sec: int = 600) -> None:
        super()._wait_until_ready(host, port, timeout_sec)
        log.info(
            "PgVector: waiting %ds for Postgres to accept connections after TCP bind",
            PGVECTOR_POST_READINESS_DELAY_SEC,
        )
        time.sleep(PGVECTOR_POST_READINESS_DELAY_SEC)

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
