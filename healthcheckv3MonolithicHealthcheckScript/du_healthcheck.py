#!/usr/bin/env python3
"""
DU Site Health Check Script (Standalone)

External dependencies (to be added to requirements.txt if not already present):
- PyYAML (yaml)
- requests (for Loki)
- kafka-python (for Kafka producer)

Purpose:
- Securely retrieve kubeconfig from a vault using cluster_id
- Connect to Kubernetes via kubectl using the kubeconfig
- Identify DU site node by siteId label
- Verify node readiness
- Filter pods (pod1, pod2) by label and verify running status and required containers
- Collect CPU/Memory/Disk metrics via kubectl for pod1 and pod2
- From pod1 logs, parse SCTP, RACH, PUCCH; perform CSR ping to <site>.csr.isp.com
- Apply thresholds from YAML configuration
- Aggregate component health to overall health status (OK/NOT_OK)
- Publish structured JSON report to Kafka; on failure, send error logs to Loki
- Logging to stdout and file with configurable level
- All external integration abstracted and secured; no secrets logged

Constraints:
- Max cyclomatic complexity 4 for all functions
- 80% functions <= 15 physical lines
- Max file length 400 lines
- Full pylint compliance (naming, docstrings)
- No CLI interface or API endpoints; script is called by an external orchestrator
"""

from __future__ import annotations

import json
import logging
import os
import re
import shlex
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import yaml  # PyYAML
import requests  # For Loki
# kafka-python is optional: provide graceful degradation if unavailable
try:
    from kafka import KafkaProducer  # type: ignore
except Exception:  # pylint: disable=broad-except
    KafkaProducer = None  # type: ignore

# =========================
# Models and Data Contracts
# =========================

@dataclass
class Thresholds:
    """Metric thresholds loaded from configuration."""
    cpu_max: float
    memory_max: float
    disk_max: float
    rach_degraded: int
    rach_degrading_min: int
    rach_healthy_min: int


@dataclass
class KafkaConfig:
    """Kafka configuration parameters."""
    brokers: List[str]
    topic: str
    tls_enabled: bool = False
    ca_path: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


@dataclass
class LokiConfig:
    """Loki configuration parameters."""
    endpoint: str
    tenant_id: Optional[str] = None


@dataclass
class PodSpec:
    """Pod and container targeting configuration."""
    name: str
    label_selector: str
    required_containers: List[str]


@dataclass
class LogPaths:
    """Log file paths for DU metrics inside pod1."""
    sctp: str
    rach: str
    pucch: str


@dataclass
class AppConfig:
    """Application configuration loaded from YAML."""
    environment: str
    log_level: str
    log_file: str
    site_label_key: str
    pods: Dict[str, PodSpec]
    log_paths: LogPaths
    thresholds: Thresholds
    kafka: KafkaConfig
    loki: LokiConfig
    ignore_containers: Dict[str, List[str]]


# =========================
# Logging Setup
# =========================

def _create_formatter() -> logging.Formatter:
    """Create a consistent, structured log formatter."""
    fmt = "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
    return logging.Formatter(fmt)


def setup_logging(level: str, file_path: str) -> None:
    """Configure logging to stdout and file."""
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    formatter = _create_formatter()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logger.level)

    file_handler = logging.FileHandler(file_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logger.level)

    logger.handlers.clear()
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)


# =========================
# Configuration Loader
# =========================

class ConfigError(Exception):
    """Raised on invalid configuration."""


def _get_env_yaml_path(env_name: str) -> str:
    """Resolve config path for the given environment."""
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, "config", f"{env_name}.yaml")


def _ensure_required(obj: Dict[str, Any], keys: List[str]) -> None:
    """Validate required keys exist in configuration dictionaries."""
    missing = [k for k in keys if k not in obj]
    if missing:
        raise ConfigError(f"Missing configuration keys: {', '.join(missing)}")


def _load_yaml(path: str) -> Dict[str, Any]:
    """Load YAML from disk with minimal risk."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# PUBLIC_INTERFACE
def load_config(env_name: str) -> AppConfig:
    """Load and validate configuration from YAML for the environment."""
    cfg_path = _get_env_yaml_path(env_name)
    raw = _load_yaml(cfg_path)

    _ensure_required(raw, [
        "environment", "logging", "kubernetes", "pods", "thresholds",
        "kafka", "loki", "log_paths"
    ])

    log_cfg = raw["logging"]
    k8s_cfg = raw["kubernetes"]
    pods_cfg = raw["pods"]
    thr = raw["thresholds"]
    kafka_cfg = raw["kafka"]
    loki_cfg = raw["loki"]
    log_paths_cfg = raw["log_paths"]
    ignore_cfg = raw.get("ignore_containers", {})

    pods: Dict[str, PodSpec] = {}
    for pname, pspec in pods_cfg.items():
        _ensure_required(pspec, ["label_selector", "required_containers"])
        pods[pname] = PodSpec(
            name=pname,
            label_selector=pspec["label_selector"],
            required_containers=list(pspec["required_containers"]),
        )

    thresholds = Thresholds(
        cpu_max=float(thr["cpu_max"]),
        memory_max=float(thr["memory_max"]),
        disk_max=float(thr["disk_max"]),
        rach_degraded=int(thr["rach_degraded"]),
        rach_degrading_min=int(thr["rach_degrading_min"]),
        rach_healthy_min=int(thr["rach_healthy_min"]),
    )

    kafka = KafkaConfig(
        brokers=list(kafka_cfg["brokers"]),
        topic=str(kafka_cfg["topic"]),
        tls_enabled=bool(kafka_cfg.get("tls_enabled", False)),
        ca_path=kafka_cfg.get("ca_path"),
        username=kafka_cfg.get("username"),
        password=kafka_cfg.get("password"),
    )

    loki = LokiConfig(
        endpoint=str(loki_cfg["endpoint"]),
        tenant_id=loki_cfg.get("tenant_id"),
    )

    log_paths = LogPaths(
        sctp=str(log_paths_cfg["sctp"]),
        rach=str(log_paths_cfg["rach"]),
        pucch=str(log_paths_cfg["pucch"]),
    )

    return AppConfig(
        environment=str(raw["environment"]),
        log_level=str(log_cfg["level"]),
        log_file=str(log_cfg["file"]),
        site_label_key=str(k8s_cfg["site_label_key"]),
        pods=pods,
        log_paths=log_paths,
        thresholds=thresholds,
        kafka=kafka,
        loki=loki,
        ignore_containers={k: list(v) for k, v in ignore_cfg.items()},
    )


# =========================
# External Integrations
# =========================

class VaultClient:
    """Abstracted vault integration for kubeconfig retrieval."""

    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)

    # PUBLIC_INTERFACE
    def get_kubeconfig(self, cluster_id: str) -> str:
        """Retrieve kubeconfig content securely for a given cluster ID.

        Placeholder implementation: Replace with actual vault SDK.
        """
        self._logger.info("Retrieving kubeconfig for cluster_id=%s", cluster_id)
        # In production, never log secrets, return content from secure store
        # Here we simulate presence via environment variable for demo
        env_key = f"KUBECONFIG_{cluster_id}"
        kubeconfig = os.environ.get(env_key)
        if not kubeconfig:
            raise RuntimeError("Kubeconfig not found in vault")
        return kubeconfig


class KubectlClient:
    """Wrapper for kubectl CLI calls using an in-memory kubeconfig."""

    def __init__(self, kubeconfig: str) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._kubeconfig = kubeconfig

    def _env(self) -> Dict[str, str]:
        """Prepare environment with kubeconfig content via KUBECONFIG file path.

        Uses a secure temporary path strategy by piping content to stdin where possible.
        Here, we write to a temp file path under process memory fs if available.
        """
        env = os.environ.copy()
        # For portability in this template, write to a temp file that caller should manage.
        # In production, use more secure approaches (e.g., ephemeral in-memory fs).
        temp_path = "/tmp/.kubeconfig_runtime"
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(self._kubeconfig)
        except OSError as exc:
            raise RuntimeError("Failed to prepare kubeconfig") from exc
        env["KUBECONFIG"] = temp_path
        return env

    def _run(self, args: List[str], timeout: int = 20) -> Tuple[int, str, str]:
        """Run kubectl command and capture output."""
        cmd = ["kubectl"] + args
        self._logger.debug("Executing: %s", shlex.join(cmd))
        try:
            proc = subprocess.run(
                cmd,
                check=False,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=self._env(),
            )
        except subprocess.TimeoutExpired:
            return 124, "", "kubectl command timeout"
        except OSError as exc:
            return 127, "", f"kubectl execution failed: {exc}"
        return proc.returncode, proc.stdout, proc.stderr

    # PUBLIC_INTERFACE
    def get_node_by_site(self, site_label_key: str, site_id: str) -> Optional[Dict[str, Any]]:
        """Get node with label site_label_key=site_id."""
        code, out, err = self._run(
            ["get", "nodes", "-o", "json", "-l", f"{site_label_key}={site_id}"]
        )
        if code != 0:
            logging.error("Failed to list nodes: %s", err.strip())
            return None
        try:
            data = json.loads(out)
        except json.JSONDecodeError:
            logging.error("Invalid JSON from kubectl get nodes")
            return None
        items = data.get("items", [])
        if not items:
            logging.warning("No nodes found with label %s=%s", site_label_key, site_id)
            return None
        if len(items) > 1:
            logging.warning("Multiple nodes matched; selecting first")
        return items[0]

    # PUBLIC_INTERFACE
    def is_node_ready(self, node: Dict[str, Any]) -> bool:
        """Check if node condition Ready is True."""
        conditions = (
            node.get("status", {}).get("conditions", []) if node else []
        )
        for cond in conditions:
            if cond.get("type") == "Ready":
                return cond.get("status") == "True"
        return False

    # PUBLIC_INTERFACE
    def list_pods(self, label_selector: str) -> List[Dict[str, Any]]:
        """List pods by label selector."""
        code, out, err = self._run(["get", "pods", "-o", "json", "-l", label_selector])
        if code != 0:
            logging.error("Failed to list pods: %s", err.strip())
            return []
        try:
            data = json.loads(out)
        except json.JSONDecodeError:
            logging.error("Invalid JSON from kubectl get pods")
            return []
        return data.get("items", [])

    # PUBLIC_INTERFACE
    def pod_phase(self, pod: Dict[str, Any]) -> str:
        """Return pod phase string."""
        return pod.get("status", {}).get("phase", "Unknown")

    # PUBLIC_INTERFACE
    def containers_status(self, pod: Dict[str, Any]) -> Dict[str, bool]:
        """Map container name to running bool for a pod."""
        statuses = pod.get("status", {}).get("containerStatuses", []) or []
        result = {}
        for st in statuses:
            name = st.get("name")
            ready = st.get("ready", False)
            started = st.get("started", ready)
            result[name] = bool(ready and started)
        return result

    # PUBLIC_INTERFACE
    def exec_cat(self, pod_name: str, container: str, path: str, namespace: Optional[str] = None) -> Tuple[bool, str]:
        """Exec into container to cat a file, return (ok, content or error)."""
        args = ["exec", pod_name]
        if namespace:
            args.extend(["-n", namespace])
        args.extend(["-c", container, "--", "cat", path])
        code, out, err = self._run(args, timeout=30)
        return (code == 0, out if code == 0 else err)

    # PUBLIC_INTERFACE
    def top_pod(self, pod_name: str, namespace: Optional[str] = None) -> Tuple[bool, str]:
        """Get kubectl top pod output (requires metrics-server)."""
        args = ["top", "pod", pod_name]
        if namespace:
            args.extend(["-n", namespace])
        code, out, err = self._run(args)
        return (code == 0, out if code == 0 else err)

    # PUBLIC_INTERFACE
    def df_pod(self, pod_name: str, container: str, namespace: Optional[str] = None) -> Tuple[bool, str]:
        """Get disk usage via exec df -h inside container."""
        args = ["exec", pod_name]
        if namespace:
            args.extend(["-n", namespace])
        args.extend(["-c", container, "--", "sh", "-lc", "df -P /"])
        code, out, err = self._run(args)
        return (code == 0, out if code == 0 else err)

    # PUBLIC_INTERFACE
    def ping_from_pod(self, pod_name: str, container: str, host: str, namespace: Optional[str] = None) -> bool:
        """Perform ICMP ping from inside a pod container."""
        args = ["exec", pod_name]
        if namespace:
            args.extend(["-n", namespace])
        args.extend(["-c", container, "--", "sh", "-lc", f"ping -c 1 -W 2 {shlex.quote(host)}"])
        code, _, _ = self._run(args, timeout=10)
        return code == 0


class KafkaProducerClient:
    """Kafka producer abstraction with minimal interface."""

    def __init__(self, cfg: KafkaConfig) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self._cfg = cfg
        self._producer = None

    def _init(self) -> None:
        """Initialize the producer once."""
        if KafkaProducer is None:
            raise RuntimeError("kafka-python not installed")
        params: Dict[str, Any] = {
            "bootstrap_servers": self._cfg.brokers,
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
            "retries": 0,
            "linger_ms": 5,
        }
        if self._cfg.tls_enabled:
            params["security_protocol"] = "SSL"
            if self._cfg.ca_path:
                params["ssl_cafile"] = self._cfg.ca_path
        if self._cfg.username and self._cfg.password:
            params["security_protocol"] = "SASL_PLAINTEXT"
            params["sasl_mechanism"] = "PLAIN"
            params["sasl_plain_username"] = self._cfg.username
            params["sasl_plain_password"] = self._cfg.password
        self._producer = KafkaProducer(**params)

    # PUBLIC_INTERFACE
    def publish(self, topic: str, message: Dict[str, Any], attempts: int = 3) -> bool:
        """Publish a message to Kafka with retry."""
        if self._producer is None:
            self._init()
        for idx in range(attempts):
            try:
                assert self._producer is not None
                fut = self._producer.send(topic, message)
                fut.get(timeout=10)
                self._producer.flush(timeout=10)
                return True
            except Exception as exc:  # pylint: disable=broad-except
                self._logger.error("Kafka publish attempt %s failed: %s", idx + 1, exc)
                time.sleep(2 ** idx)
        return False


class LokiClient:
    """Loki logging client abstraction."""

    def __init__(self, cfg: LokiConfig) -> None:
        self._cfg = cfg
        self._logger = logging.getLogger(self.__class__.__name__)

    # PUBLIC_INTERFACE
    def push_log(self, stream: str, message: str, labels: Optional[Dict[str, str]] = None) -> None:
        """Push a single log line to Loki."""
        payload = {
            "streams": [{
                "stream": {"stream": stream, **(labels or {})},
                "values": [[str(int(time.time() * 1e9)), message]],
            }]
        }
        headers = {}
        if self._cfg.tenant_id:
            headers["X-Scope-OrgID"] = self._cfg.tenant_id
        try:
            resp = requests.post(self._cfg.endpoint, json=payload, timeout=5, headers=headers, proxies={"http": None, "https": None})
            if not (200 <= resp.status_code < 300):
                self._logger.error("Failed to push to Loki: %s %s", resp.status_code, resp.text)
        except requests.RequestException as exc:
            self._logger.error("Loki push error: %s", exc)


# =========================
# Metrics and Parsing Logic
# =========================

def _parse_top_line(line: str) -> Tuple[float, float]:
    """Parse 'kubectl top pod' single line for CPU(m) and Memory(Mi)."""
    # Example: pod1   100m   200Mi
    cols = line.split()
    if len(cols) < 3:
        return 0.0, 0.0
    cpu = cols[1]
    mem = cols[2]
    cpu_m = float(cpu.rstrip("m")) if cpu.endswith("m") else float(cpu)
    mem_mi = float(mem.rstrip("Mi")) if mem.endswith("Mi") else float(mem)
    return cpu_m, mem_mi


def _parse_df_percent(df_output: str) -> float:
    """Parse df -P / output to extract used percent."""
    for line in df_output.splitlines():
        if line.strip().startswith("Filesystem"):
            continue
        parts = line.split()
        if len(parts) >= 5 and parts[4].endswith("%"):
            return float(parts[4].rstrip("%"))
    return 0.0


def _count_pattern(text: str, pattern: str) -> int:
    """Count case-insensitive pattern occurrences."""
    return len(re.findall(pattern, text, flags=re.IGNORECASE))


def _contains(text: str, pattern: str) -> bool:
    """Case-insensitive contains."""
    return re.search(pattern, text, flags=re.IGNORECASE) is not None


# PUBLIC_INTERFACE
def evaluate_rach(preamble_count: int, thr: Thresholds) -> str:
    """Evaluate RACH health from preamble count and thresholds."""
    if preamble_count >= thr.rach_healthy_min:
        return "HEALTHY"
    if preamble_count >= thr.rach_degrading_min:
        return "DEGRADING"
    return "DEGRADED"


# =========================
# Health Check Engine
# =========================

def _derive_csr_domain(site_id: str) -> str:
    """Build CSR domain from site_id."""
    return f"{site_id}.csr.isp.com"


def _first_container(required: List[str]) -> Optional[str]:
    """Pick first container from list."""
    return required[0] if required else None


def _pod_by_name(pods: List[Dict[str, Any]], name: str) -> Optional[Dict[str, Any]]:
    """Find a pod by metadata.name."""
    for p in pods:
        if p.get("metadata", {}).get("name") == name:
            return p
    return pods[0] if pods else None


def _health_from_bools(bools: List[bool]) -> str:
    """Return OK if all True, else NOT_OK."""
    return "OK" if all(bools) else "NOT_OK"


def _safe_float(val: float, max_allowed: float) -> bool:
    """Return True if val <= max_allowed."""
    return val <= max_allowed


def _node_check(kctl: KubectlClient, site_label_key: str, site_id: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """Discover node and verify readiness."""
    node = kctl.get_node_by_site(site_label_key, site_id)
    if not node:
        logging.error("Node discovery failed for site %s", site_id)
        return False, None
    if not kctl.is_node_ready(node):
        logging.error("Node not Ready for site %s", site_id)
        return False, node
    logging.info("Node %s is Ready", node.get("metadata", {}).get("name"))
    return True, node


def _filter_pods(kctl: KubectlClient, pod_spec: PodSpec) -> List[Dict[str, Any]]:
    """List pods by label and return them."""
    pods = kctl.list_pods(pod_spec.label_selector)
    return pods


def _verify_pod_running(pod: Dict[str, Any]) -> bool:
    """Check if pod phase is Running."""
    return KubectlClient.pod_phase(KubectlClient("")).__get__(None, KubectlClient) is None and False  # pragma: no cover


def _pod_running(pod: Dict[str, Any]) -> bool:
    """Helper to check pod phase without creating instance."""
    return pod.get("status", {}).get("phase") == "Running"


def _containers_ok(pod: Dict[str, Any], required: List[str], ignore: List[str]) -> bool:
    """Verify required containers are running; ignore those in ignore list."""
    statuses = pod.get("status", {}).get("containerStatuses", []) or []
    running = {}
    for st in statuses:
        name = st.get("name")
        ready = st.get("ready", False)
        started = st.get("started", ready)
        running[name] = bool(ready and started)
    needed = [c for c in required if c not in ignore]
    return all(running.get(c, False) for c in needed)


def _collect_top(kctl: KubectlClient, pod_name: str) -> Tuple[float, float]:
    """Collect CPU(m) and Memory(Mi) from kubectl top."""
    ok, out = kctl.top_pod(pod_name)
    if not ok:
        logging.error("kubectl top failed for %s: %s", pod_name, out.strip())
        return 0.0, 0.0
    lines = [ln for ln in out.splitlines() if ln and not ln.startswith("NAME")]
    if not lines:
        return 0.0, 0.0
    return _parse_top_line(lines[0])


def _collect_disk(kctl: KubectlClient, pod_name: str, container: str) -> float:
    """Collect disk usage percent using df."""
    ok, out = kctl.df_pod(pod_name, container)
    if not ok:
        logging.error("df failed for %s/%s: %s", pod_name, container, out.strip())
        return 0.0
    return _parse_df_percent(out)


def _read_log(kctl: KubectlClient, pod_name: str, container: str, path: str) -> str:
    """Read file content inside container."""
    ok, out = kctl.exec_cat(pod_name, container, path)
    if not ok:
        logging.error("Failed reading log %s in %s/%s: %s", path, pod_name, container, out.strip())
        return ""
    return out


def _parse_du_metrics(text_sctp: str, text_rach: str, text_pucch: str, thr: Thresholds) -> Dict[str, Any]:
    """Parse SCTP, RACH, PUCCH statuses from logs."""
    sctp_ok = _contains(text_sctp, r"sctp\s+healthy|sctp\s+ok|assoc\s+up")
    rach_preambles = _count_pattern(text_rach, r"preamble")
    rach_status = evaluate_rach(rach_preambles, thr)
    pucch_ok = _contains(text_pucch, r"SR\s+received")
    return {
        "sctp": "HEALTHY" if sctp_ok else "NOT_AVAILABLE",
        "rach": {"status": rach_status, "preamble_count": rach_preambles},
        "pucch": "HEALTHY" if pucch_ok else "UPLINK_FAILURE",
    }


def _csr_ping(kctl: KubectlClient, pod_name: str, container: str, site_id: str) -> bool:
    """Ping CSR domain from pod1 container."""
    host = _derive_csr_domain(site_id)
    try:
        socket.gethostbyname(host)  # quick DNS check
    except socket.gaierror:
        logging.error("CSR domain DNS failed: %s", host)
        return False
    return kctl.ping_from_pod(pod_name, container, host)


def _apply_thresholds(cpu_m: float, mem_mi: float, disk_pct: float, thr: Thresholds) -> Dict[str, str]:
    """Evaluate CPU/Memory/Disk against thresholds."""
    cpu_ok = _safe_float(cpu_m, thr.cpu_max)
    mem_ok = _safe_float(mem_mi, thr.memory_max)
    dsk_ok = _safe_float(disk_pct, thr.disk_max)
    return {
        "cpu": "OK" if cpu_ok else "NOT_OK",
        "memory": "OK" if mem_ok else "NOT_OK",
        "disk": "OK" if dsk_ok else "NOT_OK",
    }


def _aggregate(*statuses: str) -> str:
    """Aggregate multiple OK/NOT_OK statuses."""
    return "OK" if all(s == "OK" for s in statuses) else "NOT_OK"


# PUBLIC_INTERFACE
def generate_report(  # pylint: disable=too-many-arguments
    site_id: str,
    cluster_id: str,
    node_name: str,
    pod1_name: str,
    pod2_name: str,
    pod1_metrics: Dict[str, Any],
    pod2_metrics: Dict[str, Any],
    overall_status: str,
) -> Dict[str, Any]:
    """Assemble final health report structure."""
    ts = int(time.time())
    report = {
        "timestamp": ts,
        "site_id": site_id,
        "cluster_id": cluster_id,
        "node": node_name,
        "pod1": {"name": pod1_name, "metrics": pod1_metrics},
        "pod2": {"name": pod2_name, "metrics": pod2_metrics},
        "status": overall_status,
        "version": "1.0.0",
    }
    required = ["timestamp", "site_id", "cluster_id", "node", "pod1", "pod2", "status"]
    if not all(k in report for k in required):
        raise ValueError("Report missing fields")
    return report


# =========================
# Orchestration
# =========================

# PUBLIC_INTERFACE
def run_healthcheck(site_id: str, cluster_id: str, env_name: str = "dev") -> Dict[str, Any]:
    """Main orchestration for DU site health check."""
    cfg = load_config(env_name)
    setup_logging(cfg.log_level, cfg.log_file)
    logger = logging.getLogger("HealthCheck")

    vault = VaultClient()
    try:
        kubeconfig = vault.get_kubeconfig(cluster_id)
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Vault retrieval failed")
        LokiClient(cfg.loki).push_log("healthcheck", f"vault_error: {exc}", {"site": site_id})
        raise

    kctl = KubectlClient(kubeconfig)

    node_ok, node = _node_check(kctl, cfg.site_label_key, site_id)
    if not node:
        raise RuntimeError("Node not found")
    node_name = node.get("metadata", {}).get("name", "unknown")

    # Pods discovery
    pod1_spec = cfg.pods["pod1"]
    pod2_spec = cfg.pods["pod2"]
    pods1 = _filter_pods(kctl, pod1_spec)
    pods2 = _filter_pods(kctl, pod2_spec)
    pod1 = _pod_by_name(pods1, "pod1")
    pod2 = _pod_by_name(pods2, "pod2")
    if not pod1 or not pod2:
        logger.error("Required pods not found")
        LokiClient(cfg.loki).push_log("healthcheck", "pod_discovery_failed", {"site": site_id})
        raise RuntimeError("pod discovery failed")

    # Verify pods running
    pod1_name = pod1.get("metadata", {}).get("name", "pod1")
    pod2_name = pod2.get("metadata", {}).get("name", "pod2")
    pods_ok = _pod_running(pod1) and _pod_running(pod2)
    if not pods_ok:
        logger.error("Pods not running: %s/%s", pod1_name, pod2_name)

    # Verify containers
    ig1 = cfg.ignore_containers.get("pod1", [])
    ig2 = cfg.ignore_containers.get("pod2", [])
    cont1_ok = _containers_ok(pod1, pod1_spec.required_containers, ig1)
    cont2_ok = _containers_ok(pod2, pod2_spec.required_containers, ig2)

    # Metrics collection pod1
    cpu1, mem1 = _collect_top(kctl, pod1_name)
    c1 = _first_container(pod1_spec.required_containers) or ""
    disk1 = _collect_disk(kctl, pod1_name, c1) if c1 else 0.0

    # Logs and DU metrics from pod1
    sctp_log = _read_log(kctl, pod1_name, c1, cfg.log_paths.sctp) if c1 else ""
    rach_log = _read_log(kctl, pod1_name, c1, cfg.log_paths.rach) if c1 else ""
    pucch_log = _read_log(kctl, pod1_name, c1, cfg.log_paths.pucch) if c1 else ""
    du_metrics = _parse_du_metrics(sctp_log, rach_log, pucch_log, cfg.thresholds)
    csr_ok = _csr_ping(kctl, pod1_name, c1, site_id) if c1 else False

    # Threshold eval pod1
    pod1_res = _apply_thresholds(cpu1, mem1, disk1, cfg.thresholds)
    pod1_res.update({
        "sctp": du_metrics["sctp"],
        "rach": du_metrics["rach"]["status"],
        "rach_preamble_count": du_metrics["rach"]["preamble_count"],
        "pucch": du_metrics["pucch"],
        "csr_ping": "OK" if csr_ok else "NOT_OK",
        "containers": "OK" if cont1_ok else "NOT_OK",
        "pod_phase": "OK" if _pod_running(pod1) else "NOT_OK",
    })
    pod1_status = _aggregate(
        pod1_res["cpu"],
        pod1_res["memory"],
        pod1_res["disk"],
        "OK" if du_metrics["sctp"] == "HEALTHY" else "NOT_OK",
        "OK" if du_metrics["pucch"] == "HEALTHY" else "NOT_OK",
        "OK" if csr_ok else "NOT_OK",
        pod1_res["containers"],
        pod1_res["pod_phase"],
    )

    # Metrics collection pod2
    cpu2, mem2 = _collect_top(kctl, pod2_name)
    c2 = _first_container(pod2_spec.required_containers) or ""
    disk2 = _collect_disk(kctl, pod2_name, c2) if c2 else 0.0

    pod2_res = _apply_thresholds(cpu2, mem2, disk2, cfg.thresholds)
    pod2_res.update({
        "containers": "OK" if cont2_ok else "NOT_OK",
        "pod_phase": "OK" if _pod_running(pod2) else "NOT_OK",
    })
    pod2_status = _aggregate(pod2_res["cpu"], pod2_res["memory"], pod2_res["disk"], pod2_res["containers"], pod2_res["pod_phase"])

    # Overall
    node_status = "OK" if node_ok else "NOT_OK"
    overall = _aggregate(node_status, pod1_status, pod2_status)

    # Report
    report = generate_report(
        site_id=site_id,
        cluster_id=cluster_id,
        node_name=node_name,
        pod1_name=pod1_name,
        pod2_name=pod2_name,
        pod1_metrics=pod1_res,
        pod2_metrics=pod2_res,
        overall_status=overall,
    )

    # Publish to Kafka
    try:
        producer = KafkaProducerClient(cfg.kafka)
        ok = producer.publish(cfg.kafka.topic, report)
        if not ok:
            raise RuntimeError("Kafka publish failed")
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Report publish failed")
        LokiClient(cfg.loki).push_log("healthcheck", f"kafka_error: {exc}", {"site": site_id})
        # Do not raise to allow caller to still capture the report
    return report


# =========================
# Entrypoint Guard
# =========================

def _args() -> Tuple[str, str, str]:
    """Extract site_id and cluster_id (and env) from sys.argv safely."""
    if len(sys.argv) < 3:
        raise SystemExit("Usage: du_healthcheck.py <site_id> <cluster_id> [env]")
    site = sys.argv[1]
    cluster = sys.argv[2]
    env = sys.argv[3] if len(sys.argv) > 3 else os.environ.get("APP_ENV", "dev")
    return site, cluster, env


def main() -> None:
    """Main for manual execution or external invocation."""
    site_id, cluster_id, env = _args()
    report = run_healthcheck(site_id, cluster_id, env)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
