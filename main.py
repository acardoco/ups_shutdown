#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(frozen=True)
class Worker:
    host: str
    ssh_user: str = "root"
    ssh_port: int = 22
    use_sudo: bool = False


class UPSShutdownError(Exception):
    pass


class UPSShutdownController:
    def __init__(self, config: dict[str, Any], dry_run: bool = False, force: bool = False) -> None:
        self.config = config
        self.dry_run = dry_run
        self.force = force
        self.log = logging.getLogger("ups_shutdown")
        self.lock_handle = None

        kubeconfig = self.config["kubernetes"].get("kubeconfig")
        if kubeconfig:
            os.environ.setdefault("KUBECONFIG", kubeconfig)

    def acquire_lock(self) -> None:
        lock_path = Path(self.config["general"].get("lock_file", "/var/lock/ups-shutdown.lock"))
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_handle = lock_path.open("w")
        try:
            fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise UPSShutdownError(f"Another execution is already running: {lock_path}") from exc
        self.log.debug("Acquired lock: %s", lock_path)

    def run(self) -> int:
        self.acquire_lock()
        ups = self.get_ups_snapshot()
        self.log.info(
            "UPS status=%s charge=%s runtime=%s",
            ups.get("ups.status"),
            ups.get("battery.charge"),
            ups.get("battery.runtime"),
        )

        if not self.force and not self.should_trigger(ups):
            self.log.info("Threshold not reached. Nothing to do.")
            return 0

        self.log.warning("Shutdown sequence triggered.")
        self.suspend_cronjobs()
        self.scale_namespaces_down()
        self.wait_for_namespaces_to_quiet()
        self.drain_workers()
        self.poweroff_workers()
        self.wait_before_master_poweroff()
        self.poweroff_master()
        return 0

    def should_trigger(self, ups: dict[str, str]) -> bool:
        policy = self.config["ups"]
        status = ups.get("ups.status", "")
        charge = self._safe_int(ups.get("battery.charge"), default=101)
        runtime = self._safe_int(ups.get("battery.runtime"), default=10**9)

        if policy.get("require_on_battery", True) and "OB" not in status:
            return False

        charge_threshold = policy.get("shutdown_charge_lte")
        runtime_threshold = policy.get("shutdown_runtime_lte")

        charge_hit = charge_threshold is not None and charge <= int(charge_threshold)
        runtime_hit = runtime_threshold is not None and runtime <= int(runtime_threshold)

        if charge_threshold is not None and runtime_threshold is not None:
            return charge_hit or runtime_hit
        if charge_threshold is not None:
            return charge_hit
        if runtime_threshold is not None:
            return runtime_hit
        return False

    def get_ups_snapshot(self) -> dict[str, str]:
        command = [self.config["ups"].get("upsc_binary", "upsc"), self.config["ups"]["target"]]
        result = self._run_cmd(command, capture_output=True)
        data: dict[str, str] = {}
        for line in result.stdout.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            data[key.strip()] = value.strip()
        if not data:
            raise UPSShutdownError("No UPS data returned by upsc")
        return data

    def suspend_cronjobs(self) -> None:
        namespaces = self.config["kubernetes"]["app_namespaces"]
        for namespace in namespaces:
            cronjobs = self.kubectl_json(["-n", namespace, "get", "cronjobs.batch", "-o", "json"])
            for item in cronjobs.get("items", []):
                name = item["metadata"]["name"]
                self.log.info("Suspending CronJob %s/%s", namespace, name)
                self.kubectl([
                    "-n",
                    namespace,
                    "patch",
                    "cronjob.batch",
                    name,
                    "--type=merge",
                    "-p",
                    '{"spec":{"suspend":true}}',
                ])

    def scale_namespaces_down(self) -> None:
        namespaces = self.config["kubernetes"]["app_namespaces"]
        for namespace in namespaces:
            for resource_type in ("deployments.apps", "statefulsets.apps"):
                resources = self.kubectl_json(["-n", namespace, "get", resource_type, "-o", "json"])
                for item in resources.get("items", []):
                    name = item["metadata"]["name"]
                    current_replicas = int(item.get("spec", {}).get("replicas", 0) or 0)
                    if current_replicas == 0:
                        self.log.info("%s/%s already scaled to 0", namespace, name)
                        continue
                    kind = item.get("kind", resource_type)
                    self.log.info("Scaling %s %s/%s to 0", kind, namespace, name)
                    self.kubectl([
                        "-n",
                        namespace,
                        "scale",
                        kind.lower(),
                        name,
                        "--replicas=0",
                    ])

    def wait_for_namespaces_to_quiet(self) -> None:
        wait_cfg = self.config["kubernetes"].get("wait", {})
        timeout_seconds = int(wait_cfg.get("timeout_seconds", 600))
        poll_seconds = int(wait_cfg.get("poll_seconds", 10))
        ignore_owner_kinds = set(wait_cfg.get("ignore_owner_kinds", ["Cluster"]))
        ignore_pod_prefixes = tuple(wait_cfg.get("ignore_pod_prefixes", []))

        for namespace in self.config["kubernetes"]["app_namespaces"]:
            deadline = time.time() + timeout_seconds
            while time.time() < deadline:
                blocking = self._blocking_pods(namespace, ignore_owner_kinds, ignore_pod_prefixes)
                if not blocking:
                    self.log.info("Namespace %s is quiet", namespace)
                    break
                self.log.info("Waiting for namespace %s. Remaining pods: %s", namespace, ", ".join(blocking))
                time.sleep(poll_seconds)
            else:
                self.log.warning("Timeout waiting for namespace %s to become quiet", namespace)

    def _blocking_pods(
        self,
        namespace: str,
        ignore_owner_kinds: set[str],
        ignore_pod_prefixes: tuple[str, ...],
    ) -> list[str]:
        pods = self.kubectl_json(["-n", namespace, "get", "pods", "-o", "json"])
        blocking: list[str] = []
        for item in pods.get("items", []):
            name = item["metadata"]["name"]
            phase = item.get("status", {}).get("phase", "Unknown")
            if phase in {"Succeeded", "Failed"}:
                continue
            if ignore_pod_prefixes and name.startswith(ignore_pod_prefixes):
                continue
            owner_kinds = {owner.get("kind", "") for owner in item.get("metadata", {}).get("ownerReferences", [])}
            if owner_kinds and owner_kinds.issubset(ignore_owner_kinds):
                continue
            blocking.append(name)
        return blocking

    def drain_workers(self) -> None:
        drain_cfg = self.config["kubernetes"].get("drain", {})
        timeout = str(drain_cfg.get("timeout", "10m"))
        grace = str(drain_cfg.get("grace_period", 60))
        delete_emptydir = drain_cfg.get("delete_emptydir_data", True)
        force = drain_cfg.get("force", True)

        for worker in self._workers():
            self.log.info("Draining worker %s", worker.host)
            cmd = [
                "drain",
                worker.host,
                "--ignore-daemonsets",
                f"--grace-period={grace}",
                f"--timeout={timeout}",
            ]
            if delete_emptydir:
                cmd.append("--delete-emptydir-data")
            if force:
                cmd.append("--force")
            self.kubectl(cmd, check=False)

    def poweroff_workers(self) -> None:
        wait_cfg = self.config["workers"].get("poweroff", {})
        inter_worker_delay = int(wait_cfg.get("inter_worker_delay_seconds", 10))
        for worker in self._workers():
            self.log.info("Powering off worker %s", worker.host)
            command = self._worker_poweroff_command(worker)
            self._run_cmd(command, check=False)
            time.sleep(inter_worker_delay)

    def wait_before_master_poweroff(self) -> None:
        seconds = int(self.config["master"].get("pre_poweroff_delay_seconds", 60))
        if seconds > 0:
            self.log.info("Waiting %s seconds before powering off master", seconds)
            time.sleep(seconds)

    def poweroff_master(self) -> None:
        cmd = self.config["master"].get("poweroff_command", ["systemctl", "poweroff"])
        self.log.warning("Powering off master")
        self._run_cmd(cmd, check=False)

    def kubectl(self, args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
        command = list(self.config["kubernetes"].get("kubectl_cmd", ["kubectl"])) + args
        return self._run_cmd(command, check=check, capture_output=True)

    def kubectl_json(self, args: list[str]) -> dict[str, Any]:
        result = self.kubectl(args)
        stdout = result.stdout.strip() or "{}"
        return json.loads(stdout)

    def _worker_poweroff_command(self, worker: Worker) -> list[str]:
        ssh_options = self.config["workers"].get("ssh_options", [
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
        ])
        remote_command = self.config["workers"].get("poweroff_command", ["systemctl", "poweroff"])
        remote = shlex.join(remote_command)
        if worker.use_sudo:
            remote = f"sudo {remote}"
        return [
            "ssh",
            "-p",
            str(worker.ssh_port),
            *ssh_options,
            f"{worker.ssh_user}@{worker.host}",
            remote,
        ]

    def _workers(self) -> list[Worker]:
        workers: list[Worker] = []
        for item in self.config["workers"]["nodes"]:
            workers.append(
                Worker(
                    host=item["host"],
                    ssh_user=item.get("ssh_user", "root"),
                    ssh_port=int(item.get("ssh_port", 22)),
                    use_sudo=bool(item.get("use_sudo", False)),
                )
            )
        return workers

    def _run_cmd(
        self,
        command: Iterable[str],
        *,
        check: bool = True,
        capture_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        cmd_list = list(command)
        self.log.debug("Running command: %s", shlex.join(cmd_list))
        if self.dry_run:
            return subprocess.CompletedProcess(cmd_list, 0, stdout="{}\n" if capture_output else "", stderr="")

        result = subprocess.run(
            cmd_list,
            text=True,
            capture_output=capture_output,
            check=False,
        )
        if result.returncode != 0:
            msg = f"Command failed ({result.returncode}): {shlex.join(cmd_list)}"
            if result.stderr:
                msg += f" | stderr={result.stderr.strip()}"
            if check:
                raise UPSShutdownError(msg)
            self.log.warning(msg)
        return result

    @staticmethod
    def _safe_int(value: str | None, default: int) -> int:
        if value is None:
            return default
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def setup_logging(debug: bool = False) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Graceful k3s shutdown orchestrator based on NAS UPS status")
    parser.add_argument("--config", default="/etc/ups-shutdown/config.json", help="Path to JSON config")
    parser.add_argument("--dry-run", action="store_true", help="Log actions without executing them")
    parser.add_argument("--force", action="store_true", help="Run shutdown sequence regardless of UPS thresholds")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging(debug=args.debug)
    try:
        config = load_json(Path(args.config))
        controller = UPSShutdownController(config=config, dry_run=args.dry_run, force=args.force)
        return controller.run()
    except UPSShutdownError as exc:
        logging.getLogger("ups_shutdown").error(str(exc))
        return 1
    except FileNotFoundError as exc:
        logging.getLogger("ups_shutdown").error("Missing file: %s", exc)
        return 1
    except json.JSONDecodeError as exc:
        logging.getLogger("ups_shutdown").error("Invalid JSON config: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
