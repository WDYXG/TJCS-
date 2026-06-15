"""Teaching-oriented Raft with election, replication, ReadIndex, and snapshots."""

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
        """Send AppendEntries to one peer."""

    def install_snapshot(self, peer: str, request: dict) -> dict | None:
        """Send InstallSnapshot to one peer."""


class NodeRole(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"
    REMOVED = "removed"


@dataclass
class LogEntry:
    """One Raft log entry with a global index."""

    index: int
    term: int
    command: dict


class RaftNode:
    """Minimal Raft node with log compaction and teaching-style snapshots."""

    def __init__(
        self,
        node_id: str,
        peers: list[str],
        rpc_client: RaftRPCClient,
        state_machine: Any | None = None,
        storage: Any | None = None,
        node_urls: dict[str, str] | None = None,
        snapshot_threshold: int = 5,
        members: list[str] | None = None,
        peer_addresses: dict[str, str] | None = None,
    ) -> None:
        self.node_id = node_id
        self.rpc_client = rpc_client
        self.state_machine = state_machine
        self.storage = storage
        self.node_urls = node_urls or {}
        self.snapshot_threshold = snapshot_threshold
        self.members = list(dict.fromkeys(members or [node_id, *peers]))
        self.peer_addresses = peer_addresses or {peer: peer for peer in peers}
        self.peers: list[str] = []
        self._refresh_peers()

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
        self.last_included_index = 0
        self.last_included_term = 0
        self.snapshot_exists = False
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        self.heartbeat_interval = 0.5
        self._last_heartbeat_sent = 0.0
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._restore_persistent_state()
        if not self._is_voting_member():
            self.role = NodeRole.REMOVED

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
            if not self._is_voting_member():
                self.role = NodeRole.REMOVED
                return
            self.role = NodeRole.CANDIDATE
            self.current_term += 1
            election_term = self.current_term
            self.voted_for = self.node_id
            self.votes_received = {self.node_id}
            self.leader_id = None
            self._reset_election_timer()
            self._persist_state()
            request = {
                "term": election_term,
                "candidate_id": self.node_id,
                "last_log_index": self._last_log_index(),
                "last_log_term": self._term_at(self._last_log_index()) or 0,
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
                    peer: self._last_log_index() + 1 for peer in self.peers
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

            if not self._is_voting_member() or candidate_id not in self.members:
                return {"term": self.current_term, "vote_granted": False}

            local_last_index = self._last_log_index()
            local_last_term = self._term_at(local_last_index) or 0
            candidate_is_up_to_date = (last_log_term, last_log_index) >= (
                local_last_term,
                local_last_index,
            )
            vote_granted = False
            if (
                term == self.current_term
                and self.voted_for in (None, candidate_id)
                and candidate_is_up_to_date
            ):
                self.voted_for = candidate_id
                self.role = NodeRole.FOLLOWER
                self.leader_id = None
                self._reset_election_timer()
                self._persist_state()
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
                return self._append_failure()

            if term > self.current_term:
                self._become_follower(term)
            else:
                self.role = (
                    NodeRole.FOLLOWER
                    if self._is_voting_member()
                    else NodeRole.REMOVED
                )
            self.leader_id = leader_id
            self._reset_election_timer()

            if prev_log_index < self.last_included_index:
                return self._append_failure()
            if prev_log_index > self._last_log_index():
                return self._append_failure()
            if self._term_at(prev_log_index) != prev_log_term:
                return self._append_failure(max(self.last_included_index, prev_log_index - 1))

            for entry in entries:
                if entry.index <= self.last_included_index:
                    continue
                existing_term = self._term_at(entry.index)
                if existing_term is not None and existing_term != entry.term:
                    self._truncate_from(entry.index)
                if self._term_at(entry.index) is None:
                    self.log.append(entry)

            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self._last_log_index())
                self.apply_committed_entries()
            self._persist_state()

            return {
                "term": self.current_term,
                "success": True,
                "match_index": prev_log_index + len(entries),
            }

    def handle_install_snapshot(self, request: dict) -> dict:
        """Install a complete teaching-style KV snapshot."""
        term = int(request["term"])
        leader_id = str(request["leader_id"])
        snapshot_index = int(request["last_included_index"])
        snapshot_term = int(request["last_included_term"])
        kv = dict(request["kv"])

        with self._lock:
            if term < self.current_term:
                return {"term": self.current_term, "success": False}
            if term > self.current_term:
                self._become_follower(term)
            else:
                self.role = (
                    NodeRole.FOLLOWER
                    if self._is_voting_member()
                    else NodeRole.REMOVED
                )
            self.leader_id = leader_id
            self._reset_election_timer()

            if snapshot_index <= self.last_included_index:
                return {"term": self.current_term, "success": True}

            keep_suffix = self._term_at(snapshot_index) == snapshot_term
            self.log = (
                [entry for entry in self.log if entry.index > snapshot_index]
                if keep_suffix
                else []
            )
            self.last_included_index = snapshot_index
            self.last_included_term = snapshot_term
            self.snapshot_exists = True
            if request.get("members"):
                self.members = list(request["members"])
            if request.get("peer_addresses"):
                self.peer_addresses = dict(request["peer_addresses"])
            self._refresh_peers()
            self.commit_index = max(self.commit_index, snapshot_index)
            self.last_applied = max(self.last_applied, snapshot_index)
            if self.state_machine is not None:
                self.state_machine.load(kv)
            self.role = (
                NodeRole.FOLLOWER
                if self._is_voting_member()
                else NodeRole.REMOVED
            )
            self._save_snapshot(kv)
            self._persist_state()
            return {"term": self.current_term, "success": True}

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
                "last_included_index": self.last_included_index,
                "last_included_term": self.last_included_term,
                "snapshot_exists": self.snapshot_exists,
                "members": list(self.members),
                "cluster_size": len(self.members),
                "majority": self._majority(),
                "active": self._is_voting_member(),
            }

    def cluster_members(self) -> dict:
        """Return the current voting configuration."""
        with self._lock:
            return {
                "members": list(self.members),
                "cluster_size": len(self.members),
                "majority": self._majority(),
            }

    def add_node(self, request: dict) -> dict:
        """Submit one teaching-style add-node configuration entry."""
        node_id = str(request["node_id"])
        host = str(request["host"])
        port = int(request["port"])
        with self._lock:
            if self.role != NodeRole.LEADER:
                return self.not_leader_response()
            if node_id in self.members:
                return {"success": False, "error": "node already exists"}
        return self.append_command(
            {
                "type": "add_node",
                "node_id": node_id,
                "host": host,
                "port": port,
            }
        )

    def remove_node(self, request: dict) -> dict:
        """Submit one teaching-style remove-node configuration entry."""
        node_id = str(request["node_id"])
        with self._lock:
            if self.role != NodeRole.LEADER:
                return self.not_leader_response()
            if node_id not in self.members:
                return {"success": False, "error": "node not found"}
            if len(self.members) <= 1:
                return {"success": False, "error": "cannot remove last member"}
        return self.append_command({"type": "remove_node", "node_id": node_id})

    def send_heartbeats(self) -> None:
        """Send AppendEntries to all followers, empty when logs are current."""
        with self._lock:
            if self.role != NodeRole.LEADER:
                return
        for peer in self.peers:
            self._replicate_to_peer(peer)
        self._advance_commit_index()

    def append_command(self, command: dict) -> dict:
        """Append one command and attempt to replicate it."""
        with self._lock:
            if self.role != NodeRole.LEADER:
                return {
                    "success": False,
                    "error": "not leader",
                    "leader_id": self.leader_id,
                }
            entry = LogEntry(
                index=self._last_log_index() + 1,
                term=self.current_term,
                command=command,
            )
            self.log.append(entry)
            self._persist_state()

        replication_peers = list(self.peers)
        replicated_to = 1
        for peer in replication_peers:
            if self._replicate_to_peer(peer):
                replicated_to += 1
        self._advance_commit_index()
        for peer in replication_peers:
            if peer not in self.peers:
                self._replicate_to_peer(peer)
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
            base = {
                "read_index": read_result["read_index"],
                "linearizable_read": True,
            }
            if value is None:
                return {"success": False, "error": "NOT_FOUND", **base}
            return {"success": True, "key": key, "value": value, **base}

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
                return {"success": False, "error": "read quorum unavailable"}
            self.apply_committed_entries()
            return {"success": True, "read_index": self.commit_index}

    def not_leader_response(self) -> dict:
        """Return a client-friendly leader redirect hint."""
        return {
            "success": False,
            "error": "not leader",
            "leader_id": self.leader_id,
            "leader_hint": self.node_urls.get(self.leader_id),
        }

    def apply_committed_entries(self) -> None:
        """Apply committed entries in order, persist KV, and create snapshots."""
        with self._lock:
            while self.last_applied < self.commit_index:
                next_index = self.last_applied + 1
                if next_index <= self.last_included_index:
                    self.last_applied = self.last_included_index
                    continue
                entry = self._entry_at(next_index)
                if entry is None:
                    break
                if entry.command.get("type") in ("add_node", "remove_node"):
                    self._apply_membership_change(entry.command)
                elif self.state_machine is not None:
                    self.state_machine.apply(entry.command)
                self.last_applied = entry.index

            if self.storage is not None and self.state_machine is not None:
                self.storage.save_kv(self.state_machine.dump())
            if (
                self.snapshot_threshold > 0
                and self.last_applied - self.last_included_index
                >= self.snapshot_threshold
            ):
                self._create_snapshot()
            self._persist_state()

    def _replicate_to_peer(self, peer: str) -> bool:
        """Replicate missing state to one follower."""
        while True:
            with self._lock:
                if self.role != NodeRole.LEADER:
                    return False
                next_index = self.next_index.get(peer, self._last_log_index() + 1)
                if next_index <= self.last_included_index:
                    send_snapshot = True
                    request = self._snapshot_request()
                    request_term = self.current_term
                else:
                    send_snapshot = False
                    prev_log_index = next_index - 1
                    request = {
                        "term": self.current_term,
                        "leader_id": self.node_id,
                        "prev_log_index": prev_log_index,
                        "prev_log_term": self._term_at(prev_log_index) or 0,
                        "entries": [
                            asdict(entry) for entry in self._entries_from(next_index)
                        ],
                        "leader_commit": self.commit_index,
                    }
                    request_term = self.current_term

            response = (
                self.rpc_client.install_snapshot(peer, request)
                if send_snapshot
                else self.rpc_client.append_entries(peer, request)
            )
            if response is None:
                return False

            with self._lock:
                if response["term"] > self.current_term:
                    self._become_follower(response["term"])
                    return False
                if self.role != NodeRole.LEADER or self.current_term != request_term:
                    return False
                if send_snapshot and response.get("success"):
                    self.match_index[peer] = self.last_included_index
                    self.next_index[peer] = self.last_included_index + 1
                    continue
                if response.get("success"):
                    match_index = int(response.get("match_index", next_index - 1))
                    self.match_index[peer] = match_index
                    self.next_index[peer] = match_index + 1
                    return True
                if next_index <= 1:
                    return False
                self.next_index[peer] = next_index - 1

    def _send_read_heartbeat(self, peer: str, read_term: int) -> bool:
        """Send one empty AppendEntries request for a ReadIndex quorum check."""
        with self._lock:
            if self.role != NodeRole.LEADER or self.current_term != read_term:
                return False
            last_index = self._last_log_index()
            request = {
                "term": read_term,
                "leader_id": self.node_id,
                "prev_log_index": last_index,
                "prev_log_term": self._term_at(last_index) or 0,
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

    def _advance_commit_index(self) -> None:
        """Advance commit_index when a current-term entry has a majority."""
        with self._lock:
            if self.role != NodeRole.LEADER:
                return
            for index in range(self._last_log_index(), self.commit_index, -1):
                replicated = 1 + sum(
                    match >= index for match in self.match_index.values()
                )
                if (
                    replicated >= self._majority()
                    and self._term_at(index) == self.current_term
                ):
                    self.commit_index = index
                    self.apply_committed_entries()
                    return

    def _create_snapshot(self) -> None:
        snapshot_index = self.last_applied
        snapshot_term = self._term_at(snapshot_index)
        if snapshot_term is None or self.state_machine is None:
            return
        self.last_included_index = snapshot_index
        self.last_included_term = snapshot_term
        self.snapshot_exists = True
        self.log = [entry for entry in self.log if entry.index > snapshot_index]
        self._save_snapshot(self.state_machine.dump())

    def _snapshot_request(self) -> dict:
        kv = self.state_machine.dump() if self.state_machine is not None else {}
        return {
            "term": self.current_term,
            "leader_id": self.node_id,
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "kv": kv,
            "members": list(self.members),
            "peer_addresses": dict(self.peer_addresses),
        }

    def _save_snapshot(self, kv: dict) -> None:
        if self.storage is None:
            return
        self.storage.save_snapshot(
            {
                "last_included_index": self.last_included_index,
                "last_included_term": self.last_included_term,
                "kv": kv,
                "members": list(self.members),
                "peer_addresses": dict(self.peer_addresses),
            }
        )
        self.storage.save_kv(kv)

    def _restore_persistent_state(self) -> None:
        if self.storage is None:
            return
        snapshot = self.storage.load_snapshot()
        if snapshot:
            self.last_included_index = int(snapshot["last_included_index"])
            self.last_included_term = int(snapshot["last_included_term"])
            self.snapshot_exists = True
            if self.state_machine is not None:
                self.state_machine.load(snapshot.get("kv", {}))
            if snapshot.get("members"):
                self.members = list(snapshot["members"])
            if snapshot.get("peer_addresses"):
                self.peer_addresses = dict(snapshot["peer_addresses"])

        state = self.storage.load_state()
        self.current_term = int(state.get("current_term", self.current_term))
        self.voted_for = state.get("voted_for")
        if state.get("members"):
            self.members = list(state["members"])
        if state.get("peer_addresses"):
            self.peer_addresses = dict(state["peer_addresses"])
        self._refresh_peers()
        self.log = [LogEntry(**entry) for entry in state.get("log", [])]
        self.commit_index = max(int(state.get("commit_index", 0)), self.last_included_index)
        self.commit_index = min(self.commit_index, self._last_log_index())
        self.last_applied = self.last_included_index
        self.apply_committed_entries()

    def _persist_state(self) -> None:
        if self.storage is None:
            return
        self.storage.save_state(
            {
                "current_term": self.current_term,
                "voted_for": self.voted_for,
                "log": [asdict(entry) for entry in self.log],
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "last_included_index": self.last_included_index,
                "last_included_term": self.last_included_term,
                "members": list(self.members),
                "peer_addresses": dict(self.peer_addresses),
            }
        )

    def _last_log_index(self) -> int:
        return self.log[-1].index if self.log else self.last_included_index

    def _term_at(self, index: int) -> int | None:
        if index == 0 and self.last_included_index == 0:
            return 0
        if index == self.last_included_index:
            return self.last_included_term
        entry = self._entry_at(index)
        return entry.term if entry else None

    def _entry_at(self, index: int) -> LogEntry | None:
        position = index - self.last_included_index - 1
        if 0 <= position < len(self.log):
            entry = self.log[position]
            if entry.index == index:
                return entry
        return next((entry for entry in self.log if entry.index == index), None)

    def _entries_from(self, index: int) -> list[LogEntry]:
        return [entry for entry in self.log if entry.index >= index]

    def _truncate_from(self, index: int) -> None:
        self.log = [entry for entry in self.log if entry.index < index]

    def _apply_membership_change(self, command: dict) -> None:
        command_type = command["type"]
        node_id = str(command["node_id"])
        if command_type == "add_node":
            address = f"{command['host']}:{int(command['port'])}"
            if node_id not in self.members:
                self.members.append(node_id)
            self.peer_addresses[node_id] = address
            self.node_urls[node_id] = f"http://{address}"
        elif command_type == "remove_node":
            self.members = [member for member in self.members if member != node_id]
        self._refresh_peers()
        if self.role == NodeRole.LEADER:
            for peer in self.peers:
                self.next_index.setdefault(peer, self._last_log_index() + 1)
                self.match_index.setdefault(peer, 0)
        if self._is_voting_member() and self.role == NodeRole.REMOVED:
            self.role = NodeRole.FOLLOWER
        elif not self._is_voting_member():
            self.role = NodeRole.REMOVED
            self.leader_id = None

    def _refresh_peers(self) -> None:
        for member, address in self.peer_addresses.items():
            self.node_urls.setdefault(member, f"http://{address}")
        self.peers = [
            self.peer_addresses[member]
            for member in self.members
            if member != self.node_id and member in self.peer_addresses
        ]

    def _is_voting_member(self) -> bool:
        return self.node_id in self.members

    def _append_failure(self, match_index: int | None = None) -> dict:
        return {
            "term": self.current_term,
            "success": False,
            "match_index": (
                self._last_log_index() if match_index is None else match_index
            ),
        }

    def _become_follower(self, term: int) -> None:
        self.current_term = term
        self.role = (
            NodeRole.FOLLOWER if self._is_voting_member() else NodeRole.REMOVED
        )
        self.voted_for = None
        self.leader_id = None
        self.votes_received.clear()
        self._reset_election_timer()
        self._persist_state()

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
            elif role in (NodeRole.FOLLOWER, NodeRole.CANDIDATE) and election_due:
                self.start_election()

    def _reset_election_timer(self) -> None:
        self.last_heartbeat_time = time.monotonic()
        self.election_timeout = self._new_election_timeout()

    def _majority(self) -> int:
        return len(self.members) // 2 + 1

    @staticmethod
    def _new_election_timeout() -> float:
        return random.uniform(1.5, 3.0)
