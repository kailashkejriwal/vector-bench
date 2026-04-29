"""ClickHouse Docker provisioner."""

import logging
import pathlib
import shutil
import tempfile
import time
import uuid

from pydantic import SecretStr

from vectordb_bench import config
from vectordb_bench.backend.clients.clickhouse.config import ClickhouseConfig
from vectordb_bench.backend.provisioning.base import ConnectionInfo, InstanceConfig, ResourceProfile
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
    _pending_context: dict | None = None
    _trace_log_mount_dir: pathlib.Path | None = None
    _vector_cache_mount_dir: pathlib.Path | None = None

    def _get_extra_container_args(self) -> list[str]:
        """Mount CLICKHOUSE_DATA_DIR to /var/lib/clickhouse when set (e.g. NVMe disk)."""
        args: list[str] = []
        data_dir = (config.CLICKHOUSE_DATA_DIR or "").strip()
        if data_dir:
            path = pathlib.Path(data_dir)
            path.mkdir(parents=True, exist_ok=True)
            log.info("ClickHouse: using data dir on host %s (NVMe/large disk)", path)
            args.extend(["-v", f"{path}:/var/lib/clickhouse"])

        if self._should_enable_trace_log():
            args.extend(self._trace_log_mount_args())
        args.extend(self._vector_cache_mount_args())

        if getattr(config, "CLICKHOUSE_DOCKER_ENABLE_PROFILE_PERMISSIONS", False):
            args.extend(["--cap-add", "SYS_PTRACE", "--security-opt", "seccomp=unconfined"])
            log.info("ClickHouse: Docker profiling permissions enabled (SYS_PTRACE, seccomp=unconfined)")

        return args

    def _should_enable_trace_log(self) -> bool:
        if getattr(config, "CLICKHOUSE_ENABLE_TRACE_LOG", False):
            return True
        if getattr(config, "CLICKHOUSE_ENABLE_FLAMEGRAPH", False):
            return True
        db_cfg = (self._pending_context or {}).get("db_config")
        return bool(getattr(db_cfg, "enable_flamegraph", False))

    def _trace_log_mount_args(self) -> list[str]:
        flush_ms = int(getattr(config, "CLICKHOUSE_TRACE_LOG_FLUSH_INTERVAL_MS", 7500) or 7500)
        mount_root = pathlib.Path(tempfile.gettempdir()) / "vectordb_bench_clickhouse_config"
        mount_dir = mount_root / uuid.uuid4().hex
        mount_dir.mkdir(parents=True, exist_ok=True)
        trace_xml = mount_dir / "vectordb_bench_trace_log.xml"
        trace_xml.write_text(
            (
                "<clickhouse>\n"
                "  <trace_log>\n"
                "    <database>system</database>\n"
                "    <table>trace_log</table>\n"
                f"    <flush_interval_milliseconds>{flush_ms}</flush_interval_milliseconds>\n"
                "  </trace_log>\n"
                "</clickhouse>\n"
            ),
            encoding="utf-8",
        )
        self._trace_log_mount_dir = mount_dir
        log.info(
            "ClickHouse: mounted trace_log config (flush_interval_milliseconds=%s)",
            flush_ms,
        )
        return [
            "-v",
            f"{trace_xml}:/etc/clickhouse-server/config.d/vectordb_bench_trace_log.xml:ro",
        ]

    def _vector_cache_mount_args(self) -> list[str]:
        db_cfg = (self._pending_context or {}).get("db_config")
        requested = int(getattr(db_cfg, "vector_similarity_index_cache_size", 5 * 1024**3) or 0)
        mark_cache_requested = int(getattr(db_cfg, "mark_cache_size", 5 * 1024**3) or 0)
        default_value = 5 * 1024**3
        # ClickHouse docs default is 5 GiB. Only mount custom server config when user changed defaults.
        if (
            (requested <= 0 or requested == default_value)
            and (mark_cache_requested <= 0 or mark_cache_requested == default_value)
        ):
            return []

        mount_root = pathlib.Path(tempfile.gettempdir()) / "vectordb_bench_clickhouse_config"
        mount_dir = mount_root / uuid.uuid4().hex
        mount_dir.mkdir(parents=True, exist_ok=True)
        vector_cache_xml = mount_dir / "vectordb_bench_vector_cache.xml"
        xml_lines = ["<clickhouse>"]
        if requested > 0:
            xml_lines.append(
                f"  <vector_similarity_index_cache_size>{requested}</vector_similarity_index_cache_size>"
            )
        if mark_cache_requested > 0:
            xml_lines.append(f"  <mark_cache_size>{mark_cache_requested}</mark_cache_size>")
        xml_lines.append("</clickhouse>")
        vector_cache_xml.write_text(
            "\n".join(xml_lines) + "\n",
            encoding="utf-8",
        )
        self._vector_cache_mount_dir = mount_dir
        log.info(
            "ClickHouse: set vector_similarity_index_cache_size=%s bytes, mark_cache_size=%s bytes",
            requested,
            mark_cache_requested,
        )
        return [
            "-v",
            f"{vector_cache_xml}:/etc/clickhouse-server/config.d/vectordb_bench_vector_cache.xml:ro",
        ]

    def _wait_until_ready(self, host: str, port: int, timeout_sec: int = 600) -> None:
        """Wait for TCP then extra delay so ClickHouse is ready for native protocol."""
        super()._wait_until_ready(host, port, timeout_sec)
        log.info(
            "ClickHouse: waiting %ds for server to accept native protocol handshake",
            CLICKHOUSE_POST_READINESS_DELAY_SEC,
        )
        time.sleep(CLICKHOUSE_POST_READINESS_DELAY_SEC)

    def provision(
        self,
        resource_profile: ResourceProfile,
        instance_config: InstanceConfig | None = None,
        context: dict | None = None,
    ) -> ConnectionInfo:
        self._pending_context = context or {}
        try:
            return super().provision(resource_profile, instance_config, context)
        finally:
            self._pending_context = None

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

    def teardown(self, leave_running: bool = False) -> None:
        try:
            super().teardown(leave_running=leave_running)
        finally:
            if self._trace_log_mount_dir and self._trace_log_mount_dir.exists():
                try:
                    shutil.rmtree(self._trace_log_mount_dir, ignore_errors=True)
                except Exception as e:
                    log.warning("ClickHouse: failed to remove temporary trace_log mount dir: %s", e)
            self._trace_log_mount_dir = None
            if self._vector_cache_mount_dir and self._vector_cache_mount_dir.exists():
                try:
                    shutil.rmtree(self._vector_cache_mount_dir, ignore_errors=True)
                except Exception as e:
                    log.warning("ClickHouse: failed to remove temporary vector-cache mount dir: %s", e)
            self._vector_cache_mount_dir = None
