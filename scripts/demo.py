"""Run a basic three-node Raft KV demonstration."""

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from clean_data import clean_node_data


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = "scripts/default-demo-config.json"
NODE_IDS = ("node1", "node2", "node3")
NODE_URLS = {
    "node1": "http://127.0.0.1:8001",
    "node2": "http://127.0.0.1:8002",
    "node3": "http://127.0.0.1:8003",
}


def print_ok(message: str) -> None:
    print(f"[OK] {message}")


def print_fail(message: str) -> None:
    print(f"[FAIL] {message}")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)
    print_ok(message)


def request_json(
    method: str,
    url: str,
    body: dict | None = None,
    timeout: float = 3.0,
) -> tuple[int, dict]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


def start_nodes() -> dict[str, subprocess.Popen]:
    processes = {}
    for node_id in NODE_IDS:
        process = subprocess.Popen(
            [
                sys.executable,
                "src/node.py",
                "--node-id",
                node_id,
                "--config",
                DEFAULT_CONFIG_PATH,
                "serve",
            ],
            cwd=PROJECT_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        processes[node_id] = process
        print_ok(f"started {node_id}, pid={process.pid}")
    time.sleep(0.5)
    stopped = [node_id for node_id, process in processes.items() if process.poll() is not None]
    if stopped:
        stop_nodes(processes)
        raise RuntimeError(
            f"nodes exited during startup: {', '.join(stopped)}; check whether ports 8001-8003 are in use"
        )
    return processes


def stop_nodes(processes: dict[str, subprocess.Popen]) -> None:
    for node_id, process in processes.items():
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        print_ok(f"stopped {node_id}")


def get_statuses(active_nodes: tuple[str, ...] | list[str] = NODE_IDS) -> dict[str, dict]:
    statuses = {}
    for node_id in active_nodes:
        status_code, status = request_json("GET", f"{NODE_URLS[node_id]}/status")
        if status_code == 200:
            statuses[node_id] = status
    return statuses


def wait_for_leader(
    active_nodes: tuple[str, ...] | list[str] = NODE_IDS,
    excluded_leader: str | None = None,
    timeout: float = 12.0,
) -> tuple[str, dict[str, dict]]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            statuses = get_statuses(active_nodes)
            leaders = [
                node_id
                for node_id, status in statuses.items()
                if status["role"] == "leader" and node_id != excluded_leader
            ]
            if len(statuses) == len(active_nodes) and len(leaders) == 1:
                leader_id = leaders[0]
                if all(
                    status["leader_id"] == leader_id
                    for status in statuses.values()
                ):
                    return leader_id, statuses
        except (OSError, urllib.error.URLError, json.JSONDecodeError):
            pass
        time.sleep(0.25)
    raise RuntimeError("leader election timed out")


def print_statuses(title: str, statuses: dict[str, dict]) -> None:
    print(f"\n{title}")
    for node_id in sorted(statuses):
        print(f"  {node_id}: {json.dumps(statuses[node_id], ensure_ascii=False)}")


def main() -> int:
    processes: dict[str, subprocess.Popen] = {}
    try:
        print("=== Basic Raft KV Demo ===")
        clean_node_data()
        processes = start_nodes()

        leader_id, statuses = wait_for_leader()
        print_ok(f"leader elected: {leader_id}")
        print_statuses("Initial status:", statuses)
        leader_url = NODE_URLS[leader_id]

        status, response = request_json(
            "PUT", f"{leader_url}/kv/a", {"value": "1"}
        )
        require(status == 200 and response.get("success"), "PUT a=1 committed")

        status, response = request_json("GET", f"{leader_url}/kv/a")
        require(status == 200 and response.get("value") == "1", "GET a returned 1")

        status, response = request_json("DELETE", f"{leader_url}/kv/a")
        require(status == 200 and response.get("success"), "DELETE a committed")

        status, response = request_json("GET", f"{leader_url}/kv/a")
        require(
            status == 404 and response.get("error") == "NOT_FOUND",
            "GET a returned NOT_FOUND after delete",
        )

        time.sleep(1)
        statuses = get_statuses()
        require(len(statuses) == 3, "all three nodes remain available")
        require(
            len(
                {
                    (
                        status["log_length"],
                        status["commit_index"],
                        status["last_applied"],
                    )
                    for status in statuses.values()
                }
            )
            == 1,
            "all nodes have matching log and apply indexes",
        )
        print_statuses("Final status:", statuses)
        print_ok("basic demo completed")
        return 0
    except Exception as error:
        print_fail(str(error))
        return 1
    finally:
        stop_nodes(processes)


if __name__ == "__main__":
    raise SystemExit(main())
