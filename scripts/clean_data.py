"""Remove local data for the default three-node cluster."""

import shutil
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data"
NODE_IDS = ("node1", "node2", "node3")


def clean_node_data() -> None:
    """Delete data/node1, data/node2, and data/node3."""
    data_root = DATA_ROOT.resolve()
    for node_id in NODE_IDS:
        node_dir = (DATA_ROOT / node_id).resolve()
        if node_dir.parent != data_root:
            raise RuntimeError(f"Refusing to remove unexpected path: {node_dir}")
        if node_dir.exists():
            shutil.rmtree(node_dir)
        print(f"[OK] cleaned {node_dir.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    clean_node_data()
