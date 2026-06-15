"""Demonstrate Raft snapshot creation, compaction, and recovery."""

import time
import urllib.error

from clean_data import clean_node_data
from demo import (
    NODE_IDS,
    NODE_URLS,
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
from test_restart_recovery import start_node


def put_value(leader_url: str, number: int) -> None:
    status, response = request_json(
        "PUT",
        f"{leader_url}/kv/k{number}",
        {"value": str(number)},
    )
    require(
        status == 200 and response.get("success"),
        f"PUT k{number}={number} committed",
    )


def wait_for_snapshot_state(
    active_nodes: list[str] | tuple[str, ...] = NODE_IDS,
    minimum_index: int = 5,
    timeout: float = 20.0,
) -> dict[str, dict]:
    """Wait until active nodes have snapshots and matching apply indexes."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            statuses = get_statuses(active_nodes)
            if len(statuses) != len(active_nodes):
                time.sleep(0.25)
                continue
            indexes = {
                (status["commit_index"], status["last_applied"])
                for status in statuses.values()
            }
            if (
                len(indexes) == 1
                and all(status["snapshot_exists"] for status in statuses.values())
                and all(
                    status["last_included_index"] >= minimum_index
                    for status in statuses.values()
                )
            ):
                return statuses
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.25)
    raise RuntimeError("snapshot state did not converge before timeout")


def main() -> int:
    processes = {}
    try:
        print("=== Raft Snapshot Demo ===")
        clean_node_data()
        processes = start_nodes()

        leader_id, statuses = wait_for_leader()
        print_ok(f"leader elected: {leader_id}")
        print_statuses("Initial status:", statuses)
        leader_url = NODE_URLS[leader_id]

        for number in range(1, 9):
            put_value(leader_url, number)

        statuses = wait_for_snapshot_state(minimum_index=5)
        require(
            all(status["snapshot_exists"] for status in statuses.values()),
            "snapshot created",
        )
        require(
            all(status["log_length"] < 8 for status in statuses.values()),
            "log compacted",
        )
        require(
            all(
                status["commit_index"] == status["last_applied"]
                for status in statuses.values()
            ),
            "all committed entries applied after snapshot",
        )
        print_statuses("Status after first snapshot:", statuses)

        status, response = request_json("GET", f"{leader_url}/kv/k1")
        require(status == 200 and response.get("value") == "1", "GET k1 returned 1")
        status, response = request_json("GET", f"{leader_url}/kv/k8")
        require(status == 200 and response.get("value") == "8", "GET k8 returned 8")
        print_ok("snapshot recovery succeeded")

        follower_id = next(node_id for node_id in NODE_IDS if node_id != leader_id)
        processes[follower_id].terminate()
        processes[follower_id].wait(timeout=3)
        print_ok(f"killed follower {follower_id}")

        put_value(leader_url, 9)
        put_value(leader_url, 10)
        active_nodes = [node_id for node_id in NODE_IDS if node_id != follower_id]
        wait_for_snapshot_state(active_nodes, minimum_index=10)

        processes[follower_id] = start_node(follower_id)
        statuses = wait_for_snapshot_state(minimum_index=10)
        require(
            len(
                {
                    (
                        status["commit_index"],
                        status["last_applied"],
                        status["last_included_index"],
                    )
                    for status in statuses.values()
                }
            )
            == 1,
            "lagging follower caught up after snapshot",
        )
        print_statuses("Final status after follower catch-up:", statuses)

        status, response = request_json("GET", f"{leader_url}/kv/k1")
        require(status == 200 and response.get("value") == "1", "GET k1 returned 1")
        status, response = request_json("GET", f"{leader_url}/kv/k10")
        require(status == 200 and response.get("value") == "10", "GET k10 returned 10")

        print_ok("snapshot demo completed")
        return 0
    except Exception as error:
        print_fail(str(error))
        return 1
    finally:
        stop_nodes(processes)


if __name__ == "__main__":
    raise SystemExit(main())
