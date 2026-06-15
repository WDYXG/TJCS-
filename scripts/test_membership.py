"""Demonstrate teaching-style dynamic Raft membership changes."""

import shutil
import subprocess
import sys
import time
import urllib.error
from pathlib import Path

from clean_data import clean_node_data
from demo import (
    DEFAULT_CONFIG_PATH,
    NODE_IDS,
    NODE_URLS,
    PROJECT_ROOT,
    get_statuses,
    print_fail,
    print_ok,
    print_statuses,
    request_json,
    require,
    start_nodes,
    stop_nodes,
    wait_for_leader,
)


NODE4_URL = "http://127.0.0.1:8004"
ALL_NODE_URLS = {**NODE_URLS, "node4": NODE4_URL}


def clean_node4_data() -> None:
    """Remove node4's local data directory."""
    node4_dir = (PROJECT_ROOT / "data" / "node4").resolve()
    data_root = (PROJECT_ROOT / "data").resolve()
    if node4_dir.parent != data_root:
        raise RuntimeError(f"refusing to remove unexpected path: {node4_dir}")
    if node4_dir.exists():
        shutil.rmtree(node4_dir)
    print_ok("cleaned data\\node4")


def start_node4() -> subprocess.Popen:
    """Start node4 as a non-voting standby."""
    process = subprocess.Popen(
        [
            sys.executable,
            "src/node.py",
            "--node-id",
            "node4",
            "--port",
            "8004",
            "--data-dir",
            "data/node4",
            "--config",
            DEFAULT_CONFIG_PATH,
            "serve",
        ],
        cwd=PROJECT_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)
    if process.poll() is not None:
        raise RuntimeError("node4 exited during startup")
    print_ok(f"started node4 standby, pid={process.pid}")
    return process


def get_all_statuses(node_ids: list[str]) -> dict[str, dict]:
    statuses = {}
    for node_id in node_ids:
        status_code, status = request_json("GET", f"{ALL_NODE_URLS[node_id]}/status")
        if status_code == 200:
            statuses[node_id] = status
    return statuses


def wait_for_membership(
    expected_members: set[str],
    node_ids: list[str],
    require_matching_indexes: bool,
    timeout: float = 25.0,
) -> dict[str, dict]:
    """Wait until nodes see the target membership and optional matching indexes."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            statuses = get_all_statuses(node_ids)
            if len(statuses) != len(node_ids):
                time.sleep(0.25)
                continue
            if not all(set(status["members"]) == expected_members for status in statuses.values()):
                time.sleep(0.25)
                continue
            indexes = {
                (status["commit_index"], status["last_applied"])
                for status in statuses.values()
            }
            if not require_matching_indexes or len(indexes) == 1:
                return statuses
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    raise RuntimeError("membership did not converge before timeout")


def main() -> int:
    processes: dict[str, subprocess.Popen] = {}
    try:
        print("=== Raft Membership Change Demo ===")
        clean_node_data()
        clean_node4_data()
        processes = start_nodes()

        leader_id, statuses = wait_for_leader()
        print_ok(f"leader elected: {leader_id}")
        print_statuses("Initial three-node status:", statuses)
        leader_url = NODE_URLS[leader_id]

        status, response = request_json(
            "PUT", f"{leader_url}/kv/before_add", {"value": "ok"}
        )
        require(status == 200 and response.get("success"), "PUT before_add=ok committed")

        processes["node4"] = start_node4()
        status, node4_status = request_json("GET", f"{NODE4_URL}/status")
        require(
            status == 200 and node4_status.get("active") is False,
            "node4 started as non-voting standby",
        )

        status, response = request_json(
            "POST",
            f"{leader_url}/cluster/add_node",
            {"node_id": "node4", "host": "127.0.0.1", "port": 8004},
        )
        require(status == 200 and response.get("success"), "add_node log committed")

        statuses = wait_for_membership(
            {"node1", "node2", "node3", "node4"},
            ["node1", "node2", "node3", "node4"],
            require_matching_indexes=True,
        )
        status, members_response = request_json(
            "GET", f"{leader_url}/cluster/members"
        )
        require(
            status == 200 and set(members_response["members"]) == set(statuses),
            "GET /cluster/members reports node4",
        )
        print_ok("node4 added to cluster")
        print_ok("node4 caught up logs")
        require(
            all(status["cluster_size"] == 4 for status in statuses.values()),
            "cluster size changed to 4",
        )
        require(
            all(status["majority"] == 3 for status in statuses.values()),
            "cluster majority changed to 3 after adding node4",
        )
        print_statuses("Four-node status:", statuses)

        status, response = request_json(
            "PUT", f"{leader_url}/kv/after_add", {"value": "ok"}
        )
        require(status == 200 and response.get("success"), "PUT after_add=ok committed")
        for key in ("before_add", "after_add"):
            status, response = request_json("GET", f"{leader_url}/kv/{key}")
            require(status == 200 and response.get("value") == "ok", f"GET {key}=ok")

        status, response = request_json(
            "POST",
            f"{leader_url}/cluster/remove_node",
            {"node_id": "node4"},
        )
        require(status == 200 and response.get("success"), "remove_node log committed")

        statuses = wait_for_membership(
            {"node1", "node2", "node3"},
            ["node1", "node2", "node3", "node4"],
            require_matching_indexes=False,
        )
        status, members_response = request_json(
            "GET", f"{leader_url}/cluster/members"
        )
        require(
            status == 200
            and set(members_response["members"]) == {"node1", "node2", "node3"},
            "GET /cluster/members confirms node4 removal",
        )
        print_ok("node4 removed from cluster")
        require(
            statuses["node4"]["role"] == "removed"
            and statuses["node4"]["active"] is False,
            "removed node4 stopped participating in Raft",
        )
        require(
            all(statuses[node_id]["majority"] == 2 for node_id in NODE_IDS),
            "cluster majority changed back to 2 after removing node4",
        )
        print_statuses("Status after removing node4:", statuses)

        status, response = request_json(
            "PUT", f"{leader_url}/kv/after_remove", {"value": "ok"}
        )
        require(status == 200 and response.get("success"), "PUT after_remove=ok committed")
        status, response = request_json("GET", f"{leader_url}/kv/after_remove")
        require(status == 200 and response.get("value") == "ok", "GET after_remove=ok")

        print_ok("membership change demo completed")
        return 0
    except Exception as error:
        print_fail(str(error))
        return 1
    finally:
        stop_nodes(processes)


if __name__ == "__main__":
    raise SystemExit(main())
