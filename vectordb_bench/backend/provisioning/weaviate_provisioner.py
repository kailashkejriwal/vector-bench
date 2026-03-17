"""Weaviate Docker provisioner."""

import logging
import time
from urllib.request import urlopen

from pydantic import SecretStr

from vectordb_bench.backend.clients.weaviate_cloud.config import WeaviateConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo
from vectordb_bench.backend.provisioning.docker_base import (
    READINESS_POLL_INTERVAL_SEC,
    DockerContainerProvisioner,
)

log = logging.getLogger(__name__)

# Official Weaviate image; HTTP API on 8080, anonymous auth for local
WEAVIATE_IMAGE = "cr.weaviate.io/semitechnologies/weaviate:latest"
WEAVIATE_HTTP_PORT = 8080
WEAVIATE_READY_TIMEOUT_SEC = 120


class WeaviateDockerProvisioner(DockerContainerProvisioner):
    """Provision Weaviate via Docker. Exposes HTTP API on 8080 with anonymous access."""

    image = WEAVIATE_IMAGE
    container_port = WEAVIATE_HTTP_PORT
    env = [
        "AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true",
        "PERSISTENCE_DATA_PATH=/var/lib/weaviate",
        "CLUSTER_HOSTNAME=node1",
    ]

    def _wait_until_ready(self, host: str, port: int, timeout_sec: int = WEAVIATE_READY_TIMEOUT_SEC) -> None:
        """Wait until Weaviate HTTP ready endpoint returns 200 (not just TCP)."""
        url = f"http://{host}:{port}/v1/.well-known/ready"
        log.info("Waiting for Weaviate ready at %s (timeout=%ds)", url, timeout_sec)
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            try:
                with urlopen(url, timeout=5) as resp:
                    if resp.getcode() == 200:
                        log.info("Weaviate at %s is ready", url)
                        return
            except Exception as e:
                log.debug("Weaviate ready check: %s", e)
            time.sleep(READINESS_POLL_INTERVAL_SEC)
        raise RuntimeError(f"Weaviate at {url} did not return 200 within {timeout_sec}s")

    def _connection_info(self, host_port: str) -> ConnectionInfo:
        return {
            "url": f"http://{self.host}:{host_port}",
            "api_key": "",
            "no_auth": True,
        }

    @staticmethod
    def connection_info_to_db_config(conn: ConnectionInfo) -> WeaviateConfig:
        """Build WeaviateConfig from provisioner connection info."""
        return WeaviateConfig(
            url=SecretStr(conn["url"]),
            api_key=SecretStr(conn.get("api_key") or ""),
            no_auth=conn.get("no_auth", True),
        )
