"""Vespa Docker provisioner.

Vespa needs two ports: the query/document API (8080, used for search/feed after the
schema is deployed) and the config server / deploy API (19071, used once at startup to
POST the application package). Under Docker's random host-port mapping, these two
container ports map to two DIFFERENT random host ports, so both must be tracked
separately (see `extra_ports`/`extra_host_ports` on the base class).
"""

import logging
import pathlib
import time
import urllib.error
import urllib.request

from pydantic import SecretStr

from vectordb_bench import config
from vectordb_bench.backend.clients.vespa.config import VespaConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo
from vectordb_bench.backend.provisioning.docker_base import (
    READINESS_POLL_INTERVAL_SEC,
    DockerContainerProvisioner,
)

log = logging.getLogger(__name__)

# Official Vespa image; see https://hub.docker.com/r/vespaengine/vespa
VESPA_IMAGE = "vespaengine/vespa:latest"
# Query and Document API (search, feed) — only responds after an application package is deployed.
VESPA_QUERY_PORT = 8080
# Config server / deploy API — used once at startup to POST the application package.
VESPA_CONFIG_SERVER_PORT = 19071
# Default persistent data path inside the official image.
VESPA_CONTAINER_DATA_DIR = "/opt/vespa/var"


class VespaDockerProvisioner(DockerContainerProvisioner):
    """Provision Vespa via Docker (vespaengine/vespa). Exposes query API on 8080 and
    the config server / deploy API on 19071 (both published, both host-inspected)."""

    image = VESPA_IMAGE
    container_port = VESPA_QUERY_PORT
    extra_ports = [VESPA_CONFIG_SERVER_PORT]

    def _wait_until_ready(self, host: str, port: int, timeout_sec: int = 600) -> None:
        """The query port (8080) stays unresponsive until a schema is deployed (done later,
        by the Vespa client's __init__/deploy_http). What we actually need ready here is the
        CONFIG SERVER (19071), since that's what deploy_http() posts the application package to.
        Poll GET /state/v1/health on the config server's host port until it returns 200.
        """
        config_host_port = self.extra_host_ports.get(VESPA_CONFIG_SERVER_PORT)
        if not config_host_port:
            log.warning(
                "Vespa: could not resolve host port for config server (container port %s); "
                "falling back to TCP check on query port %s",
                VESPA_CONFIG_SERVER_PORT,
                port,
            )
            super()._wait_until_ready(host, port, timeout_sec=timeout_sec)
            return

        base = f"http://{host}:{config_host_port}"
        log.info(
            "Waiting for Vespa config server at %s/state/v1/health (timeout=%ds, poll=%ds)",
            base,
            timeout_sec,
            READINESS_POLL_INTERVAL_SEC,
        )
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(f"{base}/state/v1/health", timeout=5) as resp:
                    if resp.status == 200:
                        log.info("Vespa config server ready at %s", base)
                        return
            except urllib.error.HTTPError as e:
                if e.code in (401, 403):
                    raise
                # Non-200 while bootstrapping (Zookeeper quorum etc.) — retry
            except (urllib.error.URLError, TimeoutError, OSError):
                pass
            time.sleep(READINESS_POLL_INTERVAL_SEC)
        raise RuntimeError(f"Vespa config server at {base} did not become ready within {timeout_sec}s")

    def _get_extra_container_args(self) -> list[str]:
        """Mount VESPA_DATA_DIR to /opt/vespa/var when set (e.g. NVMe disk)."""
        data_dir = (config.VESPA_DATA_DIR or "").strip()
        if not data_dir:
            return []
        path = pathlib.Path(data_dir)
        path.mkdir(parents=True, exist_ok=True)
        log.info("Vespa: using data dir on host %s (NVMe/large disk)", path)
        return ["-v", f"{path}:{VESPA_CONTAINER_DATA_DIR}"]

    def _connection_info(self, host_port: str) -> ConnectionInfo:
        config_host_port = self.extra_host_ports.get(VESPA_CONFIG_SERVER_PORT, VESPA_CONFIG_SERVER_PORT)
        return {
            "url": f"http://{self.host}",
            "port": int(host_port),
            "config_port": int(config_host_port),
        }

    @staticmethod
    def connection_info_to_db_config(conn: ConnectionInfo) -> VespaConfig:
        """Build VespaConfig from provisioner connection info."""
        return VespaConfig(
            url=SecretStr(conn["url"]),
            port=conn["port"],
            config_port=conn.get("config_port", VESPA_CONFIG_SERVER_PORT),
        )
