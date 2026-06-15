"""Demonstrate ReadIndex-style linearizable GET behavior."""

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


def main() -> int:
    processes = {}
    try:
        print("=== Raft ReadIndex Demo ===")
        clean_node_data()
        processes = start_nodes()

        leader_id, statuses = wait_for_leader()
        print_ok(f"leader elected: {leader_id}")
        print_statuses("Initial status:", statuses)
        leader_url = NODE_URLS[leader_id]

        status, response = request_json(
            "PUT", f"{leader_url}/kv/read_key", {"value": "ok"}
        )
        require(status == 200 and response.get("success"), "PUT read_key=ok committed")

        before_status = get_statuses([leader_id])[leader_id]
        status, response = request_json("GET", f"{leader_url}/kv/read_key")
        require(
            status == 200
            and response.get("value") == "ok"
            and response.get("linearizable_read") is True,
            "GET read_key returned a linearizable read response",
        )
        after_status = get_statuses([leader_id])[leader_id]
        require(
            before_status["log_length"] == after_status["log_length"],
            "ReadIndex GET does not append log",
        )

        followers = [node_id for node_id in NODE_IDS if node_id != leader_id]
        stop_node(processes, followers[0])
        status, response = request_json("GET", f"{leader_url}/kv/read_key")
        require(
            status == 200
            and response.get("value") == "ok"
            and response.get("linearizable_read") is True,
            "ReadIndex GET succeeds with majority",
        )

        stop_node(processes, followers[1])
        status, response = request_json("GET", f"{leader_url}/kv/read_key")
        require(
            status == 503 and response.get("error") == "read quorum unavailable",
            "ReadIndex GET fails without majority",
        )

        statuses = get_statuses([leader_id])
        print_statuses("Final isolated Leader status:", statuses)
        print_ok("ReadIndex demo completed")
        return 0
    except Exception as error:
        print_fail(str(error))
        return 1
    finally:
        stop_nodes(processes)


if __name__ == "__main__":
    raise SystemExit(main())
