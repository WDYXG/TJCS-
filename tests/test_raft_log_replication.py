"""Tests for minimal Raft AppendEntries log replication."""

import unittest

from src.raft import LogEntry, NodeRole, RaftNode


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


class RaftLogReplicationTest(unittest.TestCase):
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
        self.leader = self.nodes["node1"]
        self.leader.start_election()

    def test_leader_replicates_and_commits_entry(self) -> None:
        result = self.leader.append_command(
            {"type": "put", "key": "a", "value": "1"}
        )

        self.assertTrue(result["success"])
        self.assertEqual(self.leader.commit_index, 1)
        for node in self.nodes.values():
            self.assertEqual(len(node.log), 1)
            self.assertEqual(node.log[0].command["key"], "a")
            self.assertEqual(node.commit_index, 1)

    def test_follower_rejects_mismatched_previous_log(self) -> None:
        follower = self.nodes["node2"]
        response = follower.handle_append_entries(
            {
                "term": 1,
                "leader_id": "node1",
                "prev_log_index": 1,
                "prev_log_term": 1,
                "entries": [],
                "leader_commit": 0,
            }
        )

        self.assertFalse(response["success"])
        self.assertEqual(response["match_index"], 0)

    def test_conflicting_follower_log_is_replaced(self) -> None:
        follower = self.nodes["node2"]
        follower.log = [
            LogEntry(index=1, term=1, command={"type": "put", "key": "old"}),
            LogEntry(index=2, term=9, command={"type": "put", "key": "wrong"}),
        ]

        response = follower.handle_append_entries(
            {
                "term": 2,
                "leader_id": "node1",
                "prev_log_index": 1,
                "prev_log_term": 1,
                "entries": [
                    {
                        "index": 2,
                        "term": 2,
                        "command": {"type": "put", "key": "new"},
                    }
                ],
                "leader_commit": 2,
            }
        )

        self.assertTrue(response["success"])
        self.assertEqual(len(follower.log), 2)
        self.assertEqual(follower.log[1].term, 2)
        self.assertEqual(follower.log[1].command["key"], "new")
        self.assertEqual(follower.commit_index, 2)

    def test_entry_commits_with_one_follower_unavailable(self) -> None:
        self.client.unavailable.add("node3")
        result = self.leader.append_command({"type": "delete", "key": "a"})

        self.assertTrue(result["success"])
        self.assertEqual(result["replicated_to"], 2)
        self.assertEqual(self.leader.role, NodeRole.LEADER)
        self.assertEqual(self.leader.commit_index, 1)

    def test_follower_rejects_vote_for_stale_candidate_log(self) -> None:
        follower = self.nodes["node2"]
        follower.log = [
            LogEntry(index=1, term=1, command={"type": "put", "key": "a"})
        ]

        response = follower.handle_request_vote(
            {
                "term": 2,
                "candidate_id": "node3",
                "last_log_index": 0,
                "last_log_term": 0,
            }
        )

        self.assertFalse(response["vote_granted"])


if __name__ == "__main__":
    unittest.main()
