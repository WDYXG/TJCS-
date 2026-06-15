"""Tests for teaching-style Raft membership changes."""

import tempfile
import unittest

from src.raft import NodeRole, RaftNode
from src.state_machine import KVStateMachine
from src.storage import JSONStorage


class InMemoryRPCClient:
    def __init__(self) -> None:
        self.nodes: dict[str, RaftNode] = {}

    def request_vote(self, peer: str, request: dict) -> dict | None:
        return self.nodes[peer].handle_request_vote(request)

    def append_entries(self, peer: str, request: dict) -> dict | None:
        return self.nodes[peer].handle_append_entries(request)

    def install_snapshot(self, peer: str, request: dict) -> dict | None:
        return self.nodes[peer].handle_install_snapshot(request)


class RaftMembershipTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_dirs = [tempfile.TemporaryDirectory() for _ in range(4)]
        self.client = InMemoryRPCClient()
        initial_members = ["node1", "node2", "node3"]
        addresses = {f"node{number}": f"node{number}" for number in range(1, 5)}
        self.nodes = {}
        for index, node_id in enumerate(addresses):
            storage = JSONStorage(self.temporary_dirs[index].name)
            self.nodes[node_id] = RaftNode(
                node_id,
                [address for member, address in addresses.items() if member != node_id],
                self.client,
                state_machine=KVStateMachine(),
                storage=storage,
                members=initial_members,
                peer_addresses=addresses,
                snapshot_threshold=0,
            )
        self.client.nodes = {**self.nodes, "node4:0": self.nodes["node4"]}
        self.leader = self.nodes["node1"]
        self.leader.start_election()

    def tearDown(self) -> None:
        for temporary_dir in self.temporary_dirs:
            temporary_dir.cleanup()

    def test_add_and_remove_node_updates_dynamic_majority(self) -> None:
        add_result = self.leader.add_node(
            {"node_id": "node4", "host": "node4", "port": 0}
        )

        self.assertTrue(add_result["success"])
        self.assertEqual(self.leader.cluster_members()["cluster_size"], 4)
        self.assertEqual(self.leader.cluster_members()["majority"], 3)
        self.assertIn("node4", self.nodes["node4"].members)
        self.assertEqual(self.nodes["node4"].commit_index, 1)

        remove_result = self.leader.remove_node({"node_id": "node4"})

        self.assertTrue(remove_result["success"])
        self.assertEqual(self.leader.cluster_members()["cluster_size"], 3)
        self.assertEqual(self.leader.cluster_members()["majority"], 2)
        self.assertEqual(self.nodes["node4"].role, NodeRole.REMOVED)
        self.assertNotIn("node4", self.nodes["node4"].members)

    def test_membership_is_restored_from_storage(self) -> None:
        self.leader.add_node({"node_id": "node4", "host": "node4", "port": 0})

        restored = RaftNode(
            "node1",
            [],
            self.client,
            state_machine=KVStateMachine(),
            storage=JSONStorage(self.temporary_dirs[0].name),
            members=["node1"],
            peer_addresses={"node1": "node1"},
            snapshot_threshold=0,
        )

        self.assertEqual(restored.members, ["node1", "node2", "node3", "node4"])
        self.assertEqual(restored.cluster_members()["majority"], 3)
        self.assertIn("node4:0", restored.peers)


if __name__ == "__main__":
    unittest.main()
