"""Run a Leader failure and recovery demonstration."""

import time

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
from clean_data import clean_node_data


def main() -> int:
    processes = {}
    try:
        print("=== Raft Leader Failover Demo ===")
        clean_node_data()
        processes = start_nodes()

        old_leader, statuses = wait_for_leader()
        print_ok(f"initial leader elected: {old_leader}")
        print_statuses("Initial status:", statuses)

        status, response = request_json(
            "PUT",
            f"{NODE_URLS[old_leader]}/kv/before_failover",
            {"value": "ok"},
        )
        require(
            status == 200 and response.get("success"),
            "PUT before_failover=ok committed",
        )

        processes[old_leader].terminate()
        processes[old_leader].wait(timeout=3)
        print_ok(f"killed leader {old_leader}")

        remaining_nodes = [node_id for node_id in NODE_IDS if node_id != old_leader]
        new_leader, statuses = wait_for_leader(
            remaining_nodes,
            excluded_leader=old_leader,
        )
        require(new_leader != old_leader, f"new leader elected: {new_leader}")

        status, response = request_json(
            "GET", f"{NODE_URLS[new_leader]}/kv/before_failover"
        )
        require(
            status == 200 and response.get("value") == "ok",
            "new leader reads before_failover=ok",
        )

        status, response = request_json(
            "PUT",
            f"{NODE_URLS[new_leader]}/kv/after_failover",
            {"value": "ok"},
        )
        require(
            status == 200 and response.get("success"),
            "PUT after_failover=ok committed with one node down",
        )

        status, response = request_json(
            "GET", f"{NODE_URLS[new_leader]}/kv/after_failover"
        )
        require(
            status == 200 and response.get("value") == "ok",
            "GET after_failover returned ok",
        )

        time.sleep(1)
        statuses = get_statuses(remaining_nodes)
        require(len(statuses) == 2, "remaining two nodes are available")
        print_statuses("Status after failover:", statuses)
        print_ok("failover demo completed")
        return 0
    except Exception as error:
        print_fail(str(error))
        return 1
    finally:
        stop_nodes(processes)


if __name__ == "__main__":
    raise SystemExit(main())
