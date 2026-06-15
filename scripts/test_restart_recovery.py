"""Run a follower restart and log catch-up demonstration."""

import subprocess
import sys
import time
import urllib.error

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


def start_node(node_id: str) -> subprocess.Popen:
    """Start one node process."""
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
    time.sleep(0.5)
    if process.poll() is not None:
        raise RuntimeError(f"{node_id} exited during restart")
    print_ok(f"restarted {node_id}, pid={process.pid}")
    return process


def wait_for_log_catch_up(timeout: float = 15.0) -> dict[str, dict]:
    """Wait until all nodes have matching log, commit, and apply indexes."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            statuses = get_statuses()
            indexes = {
                (
                    status["log_length"],
                    status["commit_index"],
                    status["last_applied"],
                )
                for status in statuses.values()
            }
            if (
                len(statuses) == 3
                and len(indexes) == 1
                and next(iter(indexes))[0] >= 2
            ):
                return statuses
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    raise RuntimeError("restarted follower did not catch up before timeout")


def main() -> int:
    processes: dict[str, subprocess.Popen] = {}
    try:
        print("=== Raft Follower Restart Recovery Demo ===")
        clean_node_data()
        processes = start_nodes()

        leader_id, statuses = wait_for_leader()
        print_ok(f"leader elected: {leader_id}")
        print_statuses("Initial status:", statuses)

        status, response = request_json(
            "PUT", f"{NODE_URLS[leader_id]}/kv/a", {"value": "1"}
        )
        require(status == 200 and response.get("success"), "PUT a=1 committed")

        follower_id = next(node_id for node_id in NODE_IDS if node_id != leader_id)
        processes[follower_id].terminate()
        processes[follower_id].wait(timeout=3)
        print_ok(f"killed follower {follower_id}")

        remaining_nodes = [node_id for node_id in NODE_IDS if node_id != follower_id]
        status, response = request_json(
            "PUT", f"{NODE_URLS[leader_id]}/kv/b", {"value": "2"}
        )
        require(
            status == 200 and response.get("success"),
            "PUT b=2 committed while one follower was down",
        )

        statuses = get_statuses(remaining_nodes)
        require(len(statuses) == 2, "remaining two nodes are available")
        print_statuses("Status while follower is down:", statuses)

        processes[follower_id] = start_node(follower_id)
        statuses = wait_for_log_catch_up()
        print_ok(f"{follower_id} caught up through AppendEntries")
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
            "all nodes have matching log, commit, and apply indexes",
        )
        print_statuses("Final status after follower restart:", statuses)

        status, response = request_json("GET", f"{NODE_URLS[leader_id]}/kv/a")
        require(status == 200 and response.get("value") == "1", "GET a returned 1")

        status, response = request_json("GET", f"{NODE_URLS[leader_id]}/kv/b")
        require(status == 200 and response.get("value") == "2", "GET b returned 2")

        print_ok("follower restart recovery demo completed")
        return 0
    except Exception as error:
        print_fail(str(error))
        return 1
    finally:
        stop_nodes(processes)


if __name__ == "__main__":
    raise SystemExit(main())
