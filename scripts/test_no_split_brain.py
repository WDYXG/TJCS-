"""Demonstrate majority progress and minority write rejection."""

import time

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


def stop_node(processes: dict, node_id: str) -> None:
    """Stop one running node."""
    process = processes[node_id]
    if process.poll() is None:
        process.terminate()
        process.wait(timeout=3)
    print_ok(f"stopped {node_id}")


def run_majority_scenario() -> dict:
    """Show that the remaining two-node majority elects one Leader."""
    print("\n=== Scenario 1: Majority Elects One Leader ===")
    clean_node_data()
    processes = start_nodes()
    try:
        old_leader, statuses = wait_for_leader()
        require(
            sum(status["role"] == "leader" for status in statuses.values()) == 1,
            f"initial cluster has exactly one leader: {old_leader}",
        )
        print_statuses("Initial status:", statuses)

        stop_node(processes, old_leader)
        remaining_nodes = [node_id for node_id in NODE_IDS if node_id != old_leader]
        new_leader, statuses = wait_for_leader(
            remaining_nodes,
            excluded_leader=old_leader,
            timeout=25.0,
        )
        require(
            sum(status["role"] == "leader" for status in statuses.values()) == 1,
            f"remaining majority elected exactly one leader: {new_leader}",
        )
        print_statuses("Majority status after old Leader stopped:", statuses)

        status, response = request_json(
            "PUT",
            f"{NODE_URLS[new_leader]}/kv/majority_ok",
            {"value": "1"},
        )
        require(
            status == 200 and response.get("success"),
            "majority committed PUT majority_ok=1",
        )
        print_ok("no split brain detected")
        return processes
    except Exception:
        stop_nodes(processes)
        raise


def run_minority_scenario() -> dict:
    """Show that an isolated node cannot commit a write."""
    print("\n=== Scenario 2: Minority Cannot Commit Writes ===")
    clean_node_data()
    processes = start_nodes()
    try:
        isolated_node, statuses = wait_for_leader()
        print_ok(f"selected current leader as isolated minority: {isolated_node}")
        print_statuses("Initial status:", statuses)

        for node_id in NODE_IDS:
            if node_id != isolated_node:
                stop_node(processes, node_id)

        time.sleep(3)
        statuses = get_statuses([isolated_node])
        require(len(statuses) == 1, f"isolated node {isolated_node} is still running")
        print_statuses("Minority status before write:", statuses)
        before_commit = statuses[isolated_node]["commit_index"]

        status, response = request_json(
            "PUT",
            f"{NODE_URLS[isolated_node]}/kv/minority_write",
            {"value": "1"},
            timeout=5,
        )
        require(
            status != 200 or not response.get("success"),
            "isolated node rejected or could not commit PUT minority_write=1",
        )

        statuses = get_statuses([isolated_node])
        require(
            statuses[isolated_node]["commit_index"] == before_commit,
            "minority commit_index did not advance",
        )
        print_statuses("Minority status after failed write:", statuses)

        status, response = request_json(
            "GET", f"{NODE_URLS[isolated_node]}/kv/minority_write"
        )
        require(
            status == 503 and response.get("error") == "read quorum unavailable",
            "minority cannot serve a linearizable GET",
        )
        print_ok("minority partition cannot commit writes")
        return processes
    except Exception:
        stop_nodes(processes)
        raise


def main() -> int:
    processes: dict = {}
    try:
        print("=== Raft No Split Brain Demo ===")
        processes = run_majority_scenario()
        stop_nodes(processes)
        processes = {}

        processes = run_minority_scenario()
        print_ok("no-split-brain demo completed")
        return 0
    except Exception as error:
        print_fail(str(error))
        return 1
    finally:
        stop_nodes(processes)


if __name__ == "__main__":
    raise SystemExit(main())
