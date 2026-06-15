"""Tests for applying committed Raft logs to the KV state machine."""

import tempfile
import unittest

from src.raft import RaftNode
from src.state_machine import KVStateMachine
from src.storage import JSONStorage


class InMemoryRPCClient:
    def __init__(self) -> None:
        self.nodes: dict[str, RaftNode] = {}
        self.unavailable: set[str] = set()

    def request_vote(self, peer: str, request: dict) -> dict | None:
        if peer in self.unavailable:
            return None
        return self.nodes[peer].handle_request_vote(request)

    def append_entries(self, peer: str, request: dict) -> dict | None:
        if peer in self.unavailable:
            return None
        return self.nodes[peer].handle_append_entries(request)


class RaftKVTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_dirs = [tempfile.TemporaryDirectory() for _ in range(3)]
        self.client = InMemoryRPCClient()
        node_ids = ("node1", "node2", "node3")
        self.nodes = {}
        for index, node_id in enumerate(node_ids):
            storage = JSONStorage(self.temporary_dirs[index].name)
            state_machine = KVStateMachine()
            self.nodes[node_id] = RaftNode(
                node_id,
                [peer for peer in node_ids if peer != node_id],
                self.client,
                state_machine=state_machine,
                storage=storage,
                node_urls={"node1": "http://127.0.0.1:8001"},
            )
        self.client.nodes = self.nodes
        self.leader = self.nodes["node1"]
        self.leader.start_election()

    def tearDown(self) -> None:
        for temporary_dir in self.temporary_dirs:
            temporary_dir.cleanup()

    def test_committed_put_is_applied_and_persisted_on_all_nodes(self) -> None:
        result = self.leader.append_command(
            {"type": "put", "key": "a", "value": "1"}
        )

        self.assertTrue(result["success"])
        for node in self.nodes.values():
            self.assertEqual(node.commit_index, 1)
            self.assertEqual(node.last_applied, 1)
            self.assertEqual(node.state_machine.get("a"), "1")
            self.assertEqual(node.storage.load_kv(), {"a": "1"})

    def test_committed_delete_is_applied_on_all_nodes(self) -> None:
        self.leader.append_command({"type": "put", "key": "a", "value": "1"})
        result = self.leader.append_command({"type": "delete", "key": "a"})

        self.assertTrue(result["success"])
        for node in self.nodes.values():
            self.assertEqual(node.last_applied, 2)
            self.assertIsNone(node.state_machine.get("a"))

    def test_get_value_only_succeeds_on_leader(self) -> None:
        self.leader.append_command({"type": "put", "key": "a", "value": "1"})

        leader_response = self.leader.get_value("a")
        self.assertEqual(leader_response["value"], "1")
        self.assertTrue(leader_response["linearizable_read"])
        self.assertEqual(leader_response["read_index"], 1)
        follower_response = self.nodes["node2"].get_value("a")
        self.assertEqual(follower_response["error"], "not leader")
        self.assertEqual(follower_response["leader_hint"], "http://127.0.0.1:8001")

    def test_read_index_does_not_append_log(self) -> None:
        self.leader.append_command({"type": "put", "key": "a", "value": "1"})
        log_length = len(self.leader.log)

        response = self.leader.get_value("a")

        self.assertTrue(response["success"])
        self.assertEqual(len(self.leader.log), log_length)

    def test_read_index_succeeds_with_one_follower_unavailable(self) -> None:
        self.leader.append_command({"type": "put", "key": "a", "value": "1"})
        self.client.unavailable.add("node3")

        response = self.leader.get_value("a")

        self.assertTrue(response["success"])
        self.assertTrue(response["linearizable_read"])

    def test_read_index_fails_without_majority(self) -> None:
        self.leader.append_command({"type": "put", "key": "a", "value": "1"})
        self.client.unavailable.update({"node2", "node3"})

        response = self.leader.get_value("a")

        self.assertFalse(response["success"])
        self.assertEqual(response["error"], "read quorum unavailable")


if __name__ == "__main__":
    unittest.main()
