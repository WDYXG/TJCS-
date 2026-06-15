"""Tests for applying committed Raft logs to the KV state machine."""

import tempfile
import unittest

from src.raft import RaftNode
from src.state_machine import KVStateMachine
from src.storage import JSONStorage


class InMemoryRPCClient:
    def __init__(self) -> None:
        self.nodes: dict[str, RaftNode] = {}

    def request_vote(self, peer: str, request: dict) -> dict | None:
        return self.nodes[peer].handle_request_vote(request)

    def append_entries(self, peer: str, request: dict) -> dict | None:
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

        self.assertEqual(self.leader.get_value("a")["value"], "1")
        follower_response = self.nodes["node2"].get_value("a")
        self.assertEqual(follower_response["error"], "not leader")
        self.assertEqual(follower_response["leader_hint"], "http://127.0.0.1:8001")


if __name__ == "__main__":
    unittest.main()
