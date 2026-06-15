"""Configuration loading for the Raft KV project."""

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class NodeConfig:
    """Configuration for one node."""

    node_id: str
    host: str = "127.0.0.1"
    port: int = 8001
    data_dir: str = "data/node1"
    peers: list[str] = field(default_factory=list)


@dataclass
class ClusterConfig:
    """Configuration for all nodes in a cluster."""

    nodes: list[NodeConfig] = field(default_factory=list)


def default_config() -> ClusterConfig:
    """Return a default local three-node cluster configuration."""
    addresses = [
        "127.0.0.1:8001",
        "127.0.0.1:8002",
        "127.0.0.1:8003",
    ]
    nodes = [
        NodeConfig(
            node_id=f"node{index + 1}",
            host="127.0.0.1",
            port=8001 + index,
            data_dir=f"data/node{index + 1}",
            peers=[address for address in addresses if address != addresses[index]],
        )
        for index in range(3)
    ]
    return ClusterConfig(nodes=nodes)


def load_config(path: str) -> ClusterConfig:
    """Load cluster configuration from JSON, or return defaults if absent."""
    config_path = Path(path)
    if not config_path.exists():
        return default_config()

    with config_path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    return ClusterConfig(
        nodes=[NodeConfig(**node_data) for node_data in data.get("nodes", [])]
    )
