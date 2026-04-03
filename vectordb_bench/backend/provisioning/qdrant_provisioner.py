"""Qdrant Docker provisioner."""

import logging
import pathlib
import time
import urllib.error
import urllib.request

from pydantic import SecretStr

from vectordb_bench import config
from vectordb_bench.backend.clients.qdrant_local.config import QdrantLocalConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo
from vectordb_bench.backend.provisioning.docker_base import (
    READINESS_POLL_INTERVAL_SEC,
    DockerContainerProvisioner,
)

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

    def _wait_until_ready(self, host: str, port: int, timeout_sec: int = 600) -> None:
        """Qdrant accepts TCP before the REST layer is ready; poll GET / until HTTP responds."""
        base = f"http://{host}:{port}"
        log.info(
            "Waiting for Qdrant HTTP API at %s (timeout=%ds, poll=%ds)",
            base,
            timeout_sec,
            READINESS_POLL_INTERVAL_SEC,
        )
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/", timeout=5) as resp:
                    if 200 <= resp.status < 500:
                        log.info("Qdrant HTTP ready at %s (status=%s)", base, resp.status)
                        # Storage / gRPC wiring can lag behind first HTTP response
                        time.sleep(2)
                        return
            except urllib.error.HTTPError as e:
                if e.code in (401, 403):
                    raise
                # 5xx while starting — retry
            except (urllib.error.URLError, TimeoutError, OSError):
                pass
            time.sleep(READINESS_POLL_INTERVAL_SEC)
        raise RuntimeError(f"Qdrant at {base} did not become HTTP-ready within {timeout_sec}s")

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
