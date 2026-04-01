"""Docker-based provisioner helpers (subprocess, no docker SDK dependency)."""

import logging
import pathlib
import re
import socket
import subprocess
import time
from typing import Any

from vectordb_bench import config

from .base import ConnectionInfo, InstanceConfig, Provisioner, ResourceProfile

log = logging.getLogger(__name__)

PROVISION_TIMEOUT_SEC = 300
TEARDOWN_TIMEOUT_SEC = 60
DEFAULT_READINESS_WAIT_SEC = 2
READINESS_POLL_INTERVAL_SEC = 2
CONTAINER_REMOVAL_POLL_SEC = 0.5


def _run(cmd: list[str], timeout: int = 60, check: bool = True) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=check,
        )
    except subprocess.CalledProcessError as e:
        log.warning("cmd=%s stderr=%s", cmd, e.stderr)
        raise
    except FileNotFoundError:
        raise RuntimeError("Docker not found. Install Docker and ensure it is on PATH.") from None


def docker_available() -> bool:
    """Return True if docker CLI is available."""
    try:
        _run(["docker", "info"], timeout=10)
        return True
    except Exception:
        return False


def _memory_for_docker(memory: str) -> str:
    """Convert Kubernetes-style memory (e.g. 4Gi, 8G) to Docker --memory format (e.g. 4g, 8g)."""
    memory = (memory or "").strip()
    if not memory:
        return "4g"
    # Docker accepts: b, k, m, g (lowercase). Kubernetes uses Gi, Mi, Ki, G, M, K.
    m = re.match(r"^(\d+(?:\.\d+)?)\s*([gGmMkKbB]i?)?$", memory)
    if not m:
        return memory
    num, unit = m.group(1), (m.group(2) or "g").lower()
    unit = unit.replace("i", "").lower()  # Gi -> g, Mi -> m, Ki -> k
    if unit == "b":
        return f"{num}b"
    if unit == "k":
        return f"{num}k"
    if unit == "m":
        return f"{num}m"
    return f"{num}g"  # g or default


def _inspect_port(container_id: str, container_port: int) -> str:
    """Get host port bound to container_port (e.g. 5432).
    Uses a template that only reads the requested port to avoid nil bindings
    when the container exposes multiple ports but we only published one.
    """
    # Single-port lookup: get HostPort for "container_port/tcp" (avoids range over all ports
    # where some entries can have nil bindings, e.g. ClickHouse 8123+9000).
    format_str = (
        "{{$p := index .NetworkSettings.Ports \"%d/tcp\"}}"
        "{{if $p}}{{(index $p 0).HostPort}}{{end}}"
    ) % container_port
    out = _run(
        [
            "docker",
            "inspect",
            "--format",
            format_str,
            container_id,
        ],
        timeout=10,
    )
    port_str = (out.stdout or "").strip()
    if port_str and port_str.isdigit():
        return port_str
    return str(container_port)


def _container_still_exists(container_id: str) -> bool:
    proc = subprocess.run(
        ["docker", "inspect", container_id],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    return proc.returncode == 0


def _wait_until_container_removed(container_id: str) -> None:
    """Block until `docker inspect` fails (container fully gone)."""
    timeout_sec = config.DOCKER_CONTAINER_REMOVAL_WAIT_TIMEOUT_SEC
    deadline = time.monotonic() + timeout_sec
    short = container_id[:12]
    log.info(
        "Teardown: waiting until container %s is absent from Docker (timeout=%ss, poll=%ss)",
        short,
        timeout_sec,
        CONTAINER_REMOVAL_POLL_SEC,
    )
    while time.monotonic() < deadline:
        if not _container_still_exists(container_id):
            log.info("Teardown: container %s confirmed removed", short)
            return
        time.sleep(CONTAINER_REMOVAL_POLL_SEC)
    if _container_still_exists(container_id):
        raise RuntimeError(
            f"Timed out after {timeout_sec}s waiting for Docker to remove container {short}"
        )


def _safe_log_filename_component(name: str, max_len: int = 80) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", (name or "image").strip())[:max_len]
    return s or "container"


def archive_container_logs_before_rm(container_id: str, image: str) -> pathlib.Path | None:
    """Write full `docker logs` to disk for post-mortem debugging. Call after stop, before rm."""
    if not getattr(config, "SAVE_PROVISIONED_CONTAINER_LOGS", False):
        return None
    dest_dir = pathlib.Path(config.PROVISIONED_CONTAINER_LOGS_DIR)
    dest_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    short = container_id[:12]
    tag = _safe_log_filename_component(image.replace("/", "_"))
    out_path = dest_dir / f"{tag}_{short}_{ts}.log"
    try:
        proc = subprocess.run(
            ["docker", "logs", container_id],
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )
        parts = [proc.stdout or "", proc.stderr or ""]
        body = "\n".join(p for p in parts if p.strip() or p == "")
        if not body.strip() and proc.returncode != 0:
            body = f"(docker logs failed, rc={proc.returncode})\n{proc.stderr or proc.stdout or ''}"
        out_path.write_text(body, encoding="utf-8")
        log.info(
            "Archived container logs to %s (docker logs rc=%s, %d bytes)",
            out_path,
            proc.returncode,
            len(body.encode("utf-8")),
        )
        return out_path
    except Exception as e:
        log.warning("Could not archive docker logs for %s: %s", short, e)
        return None


def _get_container_logs(container_id: str, tail: int | None = None) -> str:
    """Return stdout+stderr of container. Empty string if container is gone or logs unavailable."""
    args = ["docker", "logs", container_id]
    if tail is not None:
        args.extend(["--tail", str(tail)])
    try:
        out = subprocess.run(args, capture_output=True, text=True, timeout=15, check=False)
        parts = [out.stdout or "", out.stderr or ""]
        return "\n".join(p for p in parts if p.strip()).strip()
    except Exception as e:
        return f"(could not get logs: {e})"


class DockerContainerProvisioner(Provisioner):
    """Base for provisioners that run a single Docker container."""

    container_id: str | None = None
    image: str = ""
    container_port: int = 0
    env: list[str] | None = None
    command: list[str] | None = None  # optional CMD after image, e.g. ["milvus", "run", "standalone"]
    host: str = "127.0.0.1"

    def is_available(self) -> bool:
        return docker_available()

    def _get_extra_container_args(self) -> list[str]:
        """Override in subclasses to add e.g. volume mounts. Default: none."""
        return []

    def _run_container(
        self,
        resource_profile: ResourceProfile,
        extra_args: list[str] | None = None,
    ) -> str:
        """Run container with -d, return container ID. Do not use --rm so we can
        capture logs from containers that exit quickly (e.g. crash on startup)."""
        combined = list(self._get_extra_container_args())
        if extra_args:
            combined.extend(extra_args)
        args = [
            "docker",
            "run",
            "-d",
            "--pull", "always",
            "-p", str(self.container_port),  # publish to random host port
            "--cpus", resource_profile.cpu,
        ]
        if getattr(config, "PROVISION_DOCKER_MEMORY_UNLIMITED", False):
            log.info(
                "Provision step: PROVISION_DOCKER_MEMORY_UNLIMITED=1 — omitting docker --memory (host RAM only)"
            )
        else:
            args.extend(["--memory", _memory_for_docker(resource_profile.memory)])
        if self.env:
            for e in self.env:
                args.extend(["-e", e])
        if combined:
            args.extend(combined)
        args.append(self.image)
        if self.command:
            args.extend(self.command)
        log.info("Provision step: running docker run (timeout=%ds)", PROVISION_TIMEOUT_SEC)
        out = _run(args, timeout=PROVISION_TIMEOUT_SEC)
        cid = (out.stdout or "").strip()
        if not cid:
            raise RuntimeError("Docker run did not return container ID")
        return cid

    def _wait_until_ready(self, host: str, port: int, timeout_sec: int = 600) -> None:
        """Wait until the service accepts TCP connections. Override for custom readiness checks."""
        log.info("Waiting for service at %s:%s (timeout=%ds, poll every %ds)", host, port, timeout_sec, READINESS_POLL_INTERVAL_SEC)
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            try:
                with socket.create_connection((host, port), timeout=2):
                    log.info("Service at %s:%s is ready", host, port)
                    return
            except (socket.error, OSError):
                time.sleep(READINESS_POLL_INTERVAL_SEC)
        raise RuntimeError(f"Service at {host}:{port} did not become ready within {timeout_sec}s")

    def _log_container_logs(self, label: str = "Container logs", tail: int | None = 50) -> None:
        """Fetch docker logs for the current container and log them."""
        if not self.container_id:
            return
        raw = _get_container_logs(self.container_id, tail=tail)
        if not raw:
            log.info("%s: (no output)", label)
            return
        log.info("%s (id=%s):", label, self.container_id[:12])
        for line in raw.splitlines():
            log.info("  %s", line)

    def provision(
        self,
        resource_profile: ResourceProfile,
        instance_config: InstanceConfig | None = None,
        context: dict[str, Any] | None = None,
    ) -> ConnectionInfo:
        if instance_config and instance_config.use_custom_manifest and instance_config.manifest_yaml:
            raise NotImplementedError("Custom manifest for Docker provisioner not implemented yet")
        log.info("Provision step: starting container (image=%s, cpus=%s, memory=%s)", self.image, resource_profile.cpu, resource_profile.memory)
        self.container_id = self._run_container(resource_profile)
        log.info("Provision step: container started, id=%s", self.container_id[:12] if self.container_id else None)
        # Inspect port (container may exit quickly; without --rm it stays so we can inspect and get logs)
        host_port = _inspect_port(self.container_id, self.container_port)
        host_port_int = int(host_port)
        log.info("Provision step: inspected host port=%s (container_port=%s)", host_port, self.container_port)
        # Capture logs (without --rm, exited containers remain so logs are available)
        self._log_container_logs("Provision step: docker logs (startup)", tail=100)
        log.info("Provision step: waiting %ds for process to bind", DEFAULT_READINESS_WAIT_SEC)
        time.sleep(DEFAULT_READINESS_WAIT_SEC)
        try:
            self._wait_until_ready(self.host, host_port_int)
        except RuntimeError:
            self._log_container_logs("Provision step: docker logs (on readiness timeout)", tail=200)
            raise
        conn = self._connection_info(host_port)
        log.info("Provision step: complete → %s:%s", conn.get("host", self.host), conn.get("port", host_port))
        return conn

    def _connection_info(self, host_port: str) -> ConnectionInfo:
        """Subclass must return ConnectionInfo with host/port etc."""
        raise NotImplementedError

    def teardown(self, leave_running: bool = False) -> None:
        if not self.container_id:
            return
        if leave_running:
            log.info(
                "Container left running (leave_container_running=True). id=%s — interact with it: docker exec -it %s <shell>",
                self.container_id[:12],
                self.container_id[:12],
            )
            return
        cid = self.container_id
        try:
            log.info("Teardown: stopping and removing container id=%s", cid[:12])
            try:
                _run(["docker", "stop", "-t", "5", cid], timeout=TEARDOWN_TIMEOUT_SEC)
            except Exception as e:
                log.warning("Teardown stop failed (container may already have exited): %s", e)
            try:
                archive_container_logs_before_rm(cid, self.image or "")
            except Exception as e:
                log.warning("Teardown: saving container logs failed: %s", e)
            try:
                subprocess.run(
                    ["docker", "rm", "-f", cid],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                log.info("Teardown: docker rm issued for id=%s", cid[:12])
            except Exception as e:
                log.warning("Teardown rm failed: %s", e)
            _wait_until_container_removed(cid)
        finally:
            self.container_id = None
