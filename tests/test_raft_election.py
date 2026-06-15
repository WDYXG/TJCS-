"""Tests for minimal Raft election and heartbeat behavior."""

import unittest

from src.raft import NodeRole, RaftNode


class InMemoryRPCClient:
    def __init__(self) -> None:
        self.nodes: dict[str, RaftNode] = {}

    def request_vote(self, peer: str, request: dict) -> dict | None:
        return self.nodes[peer].handle_request_vote(request)

    def append_entries(self, peer: str, request: dict) -> dict | None:
        return self.nodes[peer].handle_append_entries(request)


class RaftElectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = InMemoryRPCClient()
        self.nodes = {
            node_id: RaftNode(
                node_id,
                [peer for peer in ("node1", "node2", "node3") if peer != node_id],
                self.client,
            )
            for node_id in ("node1", "node2", "node3")
        }
        self.client.nodes = self.nodes

    def test_candidate_becomes_leader_with_majority(self) -> None:
        self.nodes["node1"].start_election()

        self.assertEqual(self.nodes["node1"].role, NodeRole.LEADER)
        self.assertEqual(self.nodes["node1"].current_term, 1)
        self.assertEqual(self.nodes["node1"].leader_id, "node1")

    def test_follower_votes_once_per_term(self) -> None:
        follower = self.nodes["node3"]

        first = follower.handle_request_vote({"term": 1, "candidate_id": "node1"})
        second = follower.handle_request_vote({"term": 1, "candidate_id": "node2"})

        self.assertTrue(first["vote_granted"])
        self.assertFalse(second["vote_granted"])

    def test_heartbeat_sets_leader_and_higher_term_demotes_leader(self) -> None:
        leader = self.nodes["node1"]
        leader.start_election()
        leader.send_heartbeats()

        self.assertEqual(self.nodes["node2"].leader_id, "node1")
        self.assertEqual(self.nodes["node2"].role, NodeRole.FOLLOWER)

        self.nodes["node3"].current_term = 2
        leader.send_heartbeats()
        self.assertEqual(leader.role, NodeRole.FOLLOWER)
        self.assertEqual(leader.current_term, 2)


if __name__ == "__main__":
    unittest.main()
