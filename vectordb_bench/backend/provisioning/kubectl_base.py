"""Kubernetes (kubectl) based provisioner helpers.

Provisions a DB by applying a Deployment + Service, waiting for the Pod to be ready,
then starting a kubectl port-forward so the benchmark can connect to localhost.
Teardown stops the port-forward and deletes the resources.
"""

import logging
import subprocess
import time
from typing import Any

from .base import ConnectionInfo, InstanceConfig, Provisioner, ResourceProfile

log = logging.getLogger(__name__)

PROVISION_TIMEOUT_SEC = 300
TEARDOWN_TIMEOUT_SEC = 60
READINESS_POLL_INTERVAL_SEC = 2
DEFAULT_NAMESPACE = "vectordb-bench"


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
        raise RuntimeError("kubectl not found. Install kubectl and ensure it is on PATH.") from None


def kubectl_available() -> bool:
    """Return True if kubectl is available and can talk to a cluster."""
    try:
        _run(["kubectl", "cluster-info"], timeout=10)
        return True
    except Exception:
        return False


def _default_deployment_service_yaml(
    name: str,
    image: str,
    container_port: int,
    cpu: str,
    memory: str,
    env: list[str] | None,
    namespace: str,
) -> str:
    """Generate a minimal Deployment + Service YAML. Memory is used as-is (K8s format)."""
    env_blocks = ""
    if env:
        env_blocks = "\n".join(
            f'          - name: {e.split("=", 1)[0]}\n            value: "{e.split("=", 1)[1]}"'
            for e in env
            if "=" in e
        )
    return f"""apiVersion: v1
kind: Namespace
metadata:
  name: {namespace}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    app: {name}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: {name}
  template:
    metadata:
      labels:
        app: {name}
    spec:
      containers:
        - name: main
          image: {image}
          imagePullPolicy: Always
          ports:
            - containerPort: {container_port}
          resources:
            requests:
              cpu: "{cpu}"
              memory: "{memory}"
            limits:
              cpu: "{cpu}"
              memory: "{memory}"
{f'          env:' if env_blocks else ''}
{env_blocks}
---
apiVersion: v1
kind: Service
metadata:
  name: {name}
  namespace: {namespace}
spec:
  selector:
    app: {name}
  ports:
    - port: {container_port}
      targetPort: {container_port}
  type: ClusterIP
"""


class KubernetesContainerProvisioner(Provisioner):
    """Base for provisioners that run a single app in Kubernetes (Deployment + Service + port-forward)."""

    name: str = ""  # used as Deployment/Service name and label app=<name>
    image: str = ""
    container_port: int = 0
    env: list[str] | None = None
    namespace: str = DEFAULT_NAMESPACE
    host: str = "127.0.0.1"

    _port_forward_process: subprocess.Popen | None = None

    def is_available(self) -> bool:
        return kubectl_available()

    def _run_kubectl(self, args: list[str], timeout: int = 60, check: bool = True) -> subprocess.CompletedProcess:
        return _run(["kubectl"] + args, timeout=timeout, check=check)

    def _get_manifest(
        self,
        resource_profile: ResourceProfile,
        instance_config: InstanceConfig | None,
    ) -> str:
        """Return the YAML to apply. Override to use custom manifest or merge user YAML."""
        if instance_config and instance_config.use_custom_manifest and instance_config.manifest_yaml:
            return instance_config.manifest_yaml
        cpu = resource_profile.cpu
        memory = resource_profile.memory
        if instance_config and instance_config.resource_overrides:
            cpu = instance_config.resource_overrides.get("cpu", cpu)
            memory = instance_config.resource_overrides.get("memory", memory)
        return _default_deployment_service_yaml(
            name=self.name,
            image=self.image,
            container_port=self.container_port,
            cpu=cpu,
            memory=memory,
            env=self.env,
            namespace=self.namespace,
        )

    def _apply_manifest(self, yaml_content: str) -> None:
        log.info("Provision step: applying manifest to namespace %s", self.namespace)
        proc = subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=yaml_content,
            capture_output=True,
            text=True,
            timeout=PROVISION_TIMEOUT_SEC,
            check=True,
        )
        if proc.stdout:
            for line in proc.stdout.strip().splitlines():
                log.info("  %s", line)

    def _wait_pod_ready(self, timeout_sec: int = 600) -> None:
        log.info(
            "Provision step: waiting for pod -l app=%s in namespace %s (timeout=%ds)",
            self.name,
            self.namespace,
            timeout_sec,
        )
        self._run_kubectl(
            [
                "wait",
                "--for=condition=ready",
                "pod",
                f"-l app={self.name}",
                "-n",
                self.namespace,
                f"--timeout={timeout_sec}s",
            ],
            timeout=timeout_sec + 30,
        )
        log.info("Provision step: pod is ready")

    def _start_port_forward(self, local_port: int | None = None) -> int:
        """Start kubectl port-forward in the background. Returns the local port used."""
        port = local_port or self.container_port
        log.info("Provision step: starting port-forward %s:%s -> svc/%s:%s", self.host, port, self.name, self.container_port)
        self._port_forward_process = subprocess.Popen(
            [
                "kubectl",
                "port-forward",
                f"-n {self.namespace}",
                f"svc/{self.name}",
                f"{port}:{self.container_port}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        time.sleep(1)
        if self._port_forward_process.poll() is not None:
            stderr = (self._port_forward_process.stderr and self._port_forward_process.stderr.read()) or ""
            raise RuntimeError(f"port-forward exited immediately: {stderr}")
        log.info("Provision step: port-forward running at %s:%s", self.host, port)
        return port

    def _log_pod_logs(self, label: str = "Pod logs", tail: int = 100) -> None:
        try:
            out =             self._run_kubectl(
                ["logs", f"-l app={self.name}", "-n", self.namespace, "--tail", str(tail), "--prefix"],
                timeout=15,
                check=False,
            )
            raw = (out.stdout or "").strip()
            if not raw:
                log.info("%s: (no output)", label)
                return
            log.info("%s:", label)
            for line in raw.splitlines():
                log.info("  %s", line)
        except Exception as e:
            log.warning("%s: could not get logs: %s", label, e)

    def provision(
        self,
        resource_profile: ResourceProfile,
        instance_config: InstanceConfig | None = None,
        context: dict[str, Any] | None = None,
    ) -> ConnectionInfo:
        manifest = self._get_manifest(resource_profile, instance_config)
        self._apply_manifest(manifest)
        self._wait_pod_ready()
        self._log_pod_logs("Provision step: pod logs (startup)", tail=100)
        local_port = self._start_port_forward()
        return self._connection_info(str(local_port))

    def _connection_info(self, host_port: str) -> ConnectionInfo:
        """Subclass must return ConnectionInfo with host/port etc."""
        raise NotImplementedError

    def teardown(self) -> None:
        if self._port_forward_process is not None:
            try:
                log.info("Teardown: stopping port-forward")
                self._port_forward_process.terminate()
                self._port_forward_process.wait(timeout=10)
            except Exception as e:
                log.warning("Teardown port-forward kill failed: %s", e)
                try:
                    self._port_forward_process.kill()
                except Exception:
                    pass
            self._port_forward_process = None
        try:
            log.info("Teardown: deleting resources in namespace %s (app=%s)", self.namespace, self.name)
            self._run_kubectl(
                ["delete", "deployment,service", "-l", f"app={self.name}", "-n", self.namespace, "--ignore-not-found=true"],
                timeout=TEARDOWN_TIMEOUT_SEC,
                check=False,
            )
            log.info("Teardown: resources deleted")
        except Exception as e:
            log.warning("Teardown delete failed: %s", e)
