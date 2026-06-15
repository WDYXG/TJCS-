"""Minimal Raft leader election and heartbeat implementation."""

import random
import threading
import time
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Protocol


class RaftRPCClient(Protocol):
    """RPC operations used by RaftNode."""

    def request_vote(self, peer: str, request: dict) -> dict | None:
        """Send RequestVote to one peer."""

    def append_entries(self, peer: str, request: dict) -> dict | None:
        """Send an empty AppendEntries heartbeat to one peer."""


class NodeRole(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """One Raft log entry."""

    index: int
    term: int
    command: dict


class RaftNode:
    """Minimal Raft node supporting election, heartbeats, and log replication."""

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        rpc_client: RaftRPCClient,
        state_machine: Any | None = None,
        storage: Any | None = None,
        node_urls: dict[str, str] | None = None,
    ) -> None:
        self.node_id = node_id
        self.peers = peers
        self.current_term = 0
        self.voted_for: str | None = None
        self.role = NodeRole.FOLLOWER
        self.leader_id: str | None = None
        self.election_timeout = self._new_election_timeout()
        self.last_heartbeat_time = time.monotonic()
        self.votes_received: set[str] = set()
        self.log: list[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self.state_machine = state_machine
        self.storage = storage
        self.node_urls = node_urls or {}

        self.rpc_client = rpc_client
        self.heartbeat_interval = 0.5
        self._last_heartbeat_sent = 0.0
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start election and heartbeat timers."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop background Raft work."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=1)

    def start_election(self) -> None:
        """Become candidate, request votes, and become leader on a majority."""
        with self._lock:
            self.role = NodeRole.CANDIDATE
            self.current_term += 1
            election_term = self.current_term
            self.voted_for = self.node_id
            self.votes_received = {self.node_id}
            self.leader_id = None
            self._reset_election_timer()

        with self._lock:
            request = {
                "term": election_term,
                "candidate_id": self.node_id,
                "last_log_index": len(self.log),
                "last_log_term": self.log[-1].term if self.log else 0,
            }
        for peer in self.peers:
            response = self.rpc_client.request_vote(peer, request)
            if response is None:
                continue
            with self._lock:
                if response["term"] > self.current_term:
                    self._become_follower(response["term"])
                    return
                if self.role != NodeRole.CANDIDATE or self.current_term != election_term:
                    return
                if response.get("vote_granted"):
                    self.votes_received.add(peer)

        with self._lock:
            if (
                self.role == NodeRole.CANDIDATE
                and self.current_term == election_term
                and len(self.votes_received) >= self._majority()
            ):
                self.role = NodeRole.LEADER
                self.leader_id = self.node_id
                self.next_index = {
                    peer: len(self.log) + 1 for peer in self.peers
                }
                self.match_index = {peer: 0 for peer in self.peers}
                self._last_heartbeat_sent = 0.0

    def handle_request_vote(self, request: dict) -> dict:
        """Handle a RequestVote request."""
        term = int(request["term"])
        candidate_id = str(request["candidate_id"])
        last_log_index = int(request.get("last_log_index", 0))
        last_log_term = int(request.get("last_log_term", 0))

        with self._lock:
            if term > self.current_term:
                self._become_follower(term)

            vote_granted = False
            local_last_term = self.log[-1].term if self.log else 0
            candidate_is_up_to_date = (last_log_term, last_log_index) >= (
                local_last_term,
                len(self.log),
            )
            if (
                term == self.current_term
                and self.voted_for in (None, candidate_id)
                and candidate_is_up_to_date
            ):
                self.voted_for = candidate_id
                self.role = NodeRole.FOLLOWER
                self.leader_id = None
                self._reset_election_timer()
                vote_granted = True

            return {"term": self.current_term, "vote_granted": vote_granted}

    def handle_append_entries(self, request: dict) -> dict:
        """Handle AppendEntries for both log replication and heartbeats."""
        term = int(request["term"])
        leader_id = str(request["leader_id"])
        prev_log_index = int(request.get("prev_log_index", 0))
        prev_log_term = int(request.get("prev_log_term", 0))
        entries = [LogEntry(**entry) for entry in request.get("entries", [])]
        leader_commit = int(request.get("leader_commit", 0))

        with self._lock:
            if term < self.current_term:
                return {
                    "term": self.current_term,
                    "success": False,
                    "match_index": len(self.log),
                }

            if term > self.current_term:
                self._become_follower(term)
            else:
                self.role = NodeRole.FOLLOWER

            self.leader_id = leader_id
            self._reset_election_timer()

            if prev_log_index > len(self.log):
                return {
                    "term": self.current_term,
                    "success": False,
                    "match_index": len(self.log),
                }
            if (
                prev_log_index > 0
                and self.log[prev_log_index - 1].term != prev_log_term
            ):
                return {
                    "term": self.current_term,
                    "success": False,
                    "match_index": prev_log_index - 1,
                }

            for entry in entries:
                position = entry.index - 1
                if position < len(self.log):
                    if self.log[position].term != entry.term:
                        self.log = self.log[:position]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
                self.apply_committed_entries()

            match_index = prev_log_index + len(entries)
            return {
                "term": self.current_term,
                "success": True,
                "match_index": match_index,
            }

    def status(self) -> dict:
        """Return public node status."""
        with self._lock:
            return {
                "node_id": self.node_id,
                "role": self.role.value,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
            }

    def send_heartbeats(self) -> None:
        """Send AppendEntries to all followers, empty when logs are current."""
        with self._lock:
            if self.role != NodeRole.LEADER:
                return

        for peer in self.peers:
            self._replicate_to_peer(peer)
        self._advance_commit_index()

    def append_command(self, command: dict) -> dict:
        """Append one debug command and attempt to replicate it."""
        with self._lock:
            if self.role != NodeRole.LEADER:
                return {
                    "success": False,
                    "error": "not leader",
                    "leader_id": self.leader_id,
                }
            entry = LogEntry(
                index=len(self.log) + 1,
                term=self.current_term,
                command=command,
            )
            self.log.append(entry)

        replicated_to = 1
        for peer in self.peers:
            if self._replicate_to_peer(peer):
                replicated_to += 1

        self._advance_commit_index()
        self.send_heartbeats()
        with self._lock:
            return {
                "success": self.commit_index >= entry.index,
                "log_length": len(self.log),
                "commit_index": self.commit_index,
                "replicated_to": replicated_to,
            }

    def get_value(self, key: str) -> dict:
        """Read one value after confirming the leader still has a quorum."""
        read_result = self.ensure_read_quorum()
        if not read_result["success"]:
            return read_result

        with self._lock:
            if self.state_machine is None:
                return {"success": False, "error": "state machine unavailable"}
            value = self.state_machine.get(key)
            if value is None:
                return {
                    "success": False,
                    "error": "NOT_FOUND",
                    "read_index": read_result["read_index"],
                    "linearizable_read": True,
                }
            return {
                "success": True,
                "key": key,
                "value": value,
                "read_index": read_result["read_index"],
                "linearizable_read": True,
            }

    def ensure_read_quorum(self) -> dict:
        """Confirm current leadership with a majority before serving a read."""
        with self._lock:
            if self.role != NodeRole.LEADER:
                return self.not_leader_response()
            read_term = self.current_term

        successful_nodes = 1
        for peer in self.peers:
            if self._send_read_heartbeat(peer, read_term):
                successful_nodes += 1

        with self._lock:
            if self.role != NodeRole.LEADER or self.current_term != read_term:
                return self.not_leader_response()
            if successful_nodes < self._majority():
                return {
                    "success": False,
                    "error": "read quorum unavailable",
                }
            self.apply_committed_entries()
            return {
                "success": True,
                "read_index": self.commit_index,
            }

    def _send_read_heartbeat(self, peer: str, read_term: int) -> bool:
        """Send one empty AppendEntries request for a ReadIndex quorum check."""
        with self._lock:
            if self.role != NodeRole.LEADER or self.current_term != read_term:
                return False
            prev_log_index = len(self.log)
            request = {
                "term": read_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": self.log[-1].term if self.log else 0,
                "entries": [],
                "leader_commit": self.commit_index,
            }

        response = self.rpc_client.append_entries(peer, request)
        if response is None:
            return False

        with self._lock:
            if response["term"] > self.current_term:
                self._become_follower(response["term"])
                return False
            return (
                self.role == NodeRole.LEADER
                and self.current_term == read_term
                and bool(response.get("success"))
            )

    def not_leader_response(self) -> dict:
        """Return a client-friendly leader redirect hint."""
        return {
            "success": False,
            "error": "not leader",
            "leader_id": self.leader_id,
            "leader_hint": self.node_urls.get(self.leader_id),
        }

    def apply_committed_entries(self) -> None:
        """Apply committed log entries in order and persist KV state."""
        with self._lock:
            while self.last_applied < self.commit_index:
                entry = self.log[self.last_applied]
                if self.state_machine is not None:
                    self.state_machine.apply(entry.command)
                self.last_applied = entry.index
            if self.storage is not None and self.state_machine is not None:
                self.storage.save_kv(self.state_machine.dump())

    def _replicate_to_peer(self, peer: str) -> bool:
        """Replicate missing log entries to one follower."""
        while True:
            with self._lock:
                if self.role != NodeRole.LEADER:
                    return False
                next_index = self.next_index.get(peer, len(self.log) + 1)
                prev_log_index = next_index - 1
                prev_log_term = (
                    self.log[prev_log_index - 1].term if prev_log_index > 0 else 0
                )
                request = {
                    "term": self.current_term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": [
                        asdict(entry) for entry in self.log[next_index - 1 :]
                    ],
                    "leader_commit": self.commit_index,
                }
                request_term = self.current_term

            response = self.rpc_client.append_entries(peer, request)
            if response is None:
                return False

            with self._lock:
                if response["term"] > self.current_term:
                    self._become_follower(response["term"])
                    return False
                if self.role != NodeRole.LEADER or self.current_term != request_term:
                    return False
                if response.get("success"):
                    match_index = int(response.get("match_index", prev_log_index))
                    self.match_index[peer] = match_index
                    self.next_index[peer] = match_index + 1
                    return True
                if next_index <= 1:
                    return False
                self.next_index[peer] = next_index - 1

    def _advance_commit_index(self) -> None:
        """Advance commit_index when a current-term entry has a majority."""
        with self._lock:
            if self.role != NodeRole.LEADER:
                return
            for index in range(len(self.log), self.commit_index, -1):
                replicated = 1 + sum(
                    match >= index for match in self.match_index.values()
                )
                if (
                    replicated >= self._majority()
                    and self.log[index - 1].term == self.current_term
                ):
                    self.commit_index = index
                    self.apply_committed_entries()
                    return

    def _run(self) -> None:
        while not self._stop_event.wait(0.05):
            now = time.monotonic()
            with self._lock:
                role = self.role
                election_due = now - self.last_heartbeat_time >= self.election_timeout
                heartbeat_due = now - self._last_heartbeat_sent >= self.heartbeat_interval

            if role == NodeRole.LEADER and heartbeat_due:
                self.send_heartbeats()
                with self._lock:
                    self._last_heartbeat_sent = time.monotonic()
            elif role != NodeRole.LEADER and election_due:
                self.start_election()

    def _become_follower(self, term: int) -> None:
        self.current_term = term
        self.role = NodeRole.FOLLOWER
        self.voted_for = None
        self.leader_id = None
        self.votes_received.clear()
        self._reset_election_timer()

    def _reset_election_timer(self) -> None:
        self.last_heartbeat_time = time.monotonic()
        self.election_timeout = self._new_election_timeout()

    def _majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    @staticmethod
    def _new_election_timeout() -> float:
        return random.uniform(1.5, 3.0)
