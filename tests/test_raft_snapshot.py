"""Tests for teaching-style Raft snapshots."""

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

    def install_snapshot(self, peer: str, request: dict) -> dict | None:
        if peer in self.unavailable:
            return None
        return self.nodes[peer].handle_install_snapshot(request)


class RaftSnapshotTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_dirs = [tempfile.TemporaryDirectory() for _ in range(3)]
        self.client = InMemoryRPCClient()
        node_ids = ("node1", "node2", "node3")
        self.nodes = {}
        for index, node_id in enumerate(node_ids):
            self.nodes[node_id] = self.make_node(node_id, node_ids, index)
        self.client.nodes = self.nodes
        self.leader = self.nodes["node1"]
        self.leader.start_election()

    def tearDown(self) -> None:
        for temporary_dir in self.temporary_dirs:
            temporary_dir.cleanup()

    def make_node(self, node_id: str, node_ids: tuple[str, ...], index: int) -> RaftNode:
        storage = JSONStorage(self.temporary_dirs[index].name)
        state_machine = KVStateMachine()
        state_machine.load(storage.load_kv())
        return RaftNode(
            node_id,
            [peer for peer in node_ids if peer != node_id],
            self.client,
            state_machine=state_machine,
            storage=storage,
            snapshot_threshold=5,
        )

    def append_values(self, start: int, end: int) -> None:
        for number in range(start, end + 1):
            result = self.leader.append_command(
                {"type": "put", "key": f"k{number}", "value": str(number)}
            )
            self.assertTrue(result["success"])

    def test_snapshot_compacts_log_and_persists_metadata(self) -> None:
        self.append_values(1, 8)

        for node in self.nodes.values():
            self.assertTrue(node.snapshot_exists)
            self.assertGreaterEqual(node.last_included_index, 5)
            self.assertLess(len(node.log), 8)
            snapshot = node.storage.load_snapshot()
            self.assertEqual(snapshot["last_included_index"], node.last_included_index)
            self.assertEqual(snapshot["kv"]["k1"], "1")

    def test_lagging_follower_receives_install_snapshot(self) -> None:
        self.append_values(1, 8)
        self.client.unavailable.add("node3")
        self.append_values(9, 10)

        old_follower = self.nodes["node3"]
        old_follower.log = []
        old_follower.last_included_index = 0
        old_follower.last_included_term = 0
        old_follower.snapshot_exists = False
        self.leader.next_index["node3"] = 1
        self.client.unavailable.remove("node3")
        self.leader.send_heartbeats()

        self.assertEqual(old_follower.last_included_index, 10)
        self.assertEqual(old_follower.commit_index, 10)
        self.assertEqual(old_follower.last_applied, 10)
        self.assertEqual(old_follower.state_machine.get("k10"), "10")

    def test_restart_restores_snapshot_and_committed_suffix(self) -> None:
        self.append_values(1, 8)

        restored = self.make_node("node2", ("node1", "node2", "node3"), 1)

        self.assertTrue(restored.snapshot_exists)
        self.assertEqual(restored.commit_index, 8)
        self.assertEqual(restored.last_applied, 8)
        self.assertEqual(restored.state_machine.get("k1"), "1")
        self.assertEqual(restored.state_machine.get("k8"), "8")


if __name__ == "__main__":
    unittest.main()
