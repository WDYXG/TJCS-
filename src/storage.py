"""JSON file persistence for node state and KV data."""

import json
import os
import tempfile
from pathlib import Path


class JSONStorage:
    """Store node state and KV data as JSON files."""

    def __init__(self, data_dir: str) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.data_dir / "state.json"
        self.kv_path = self.data_dir / "kv.json"

    def save_state(self, state_dict: dict) -> None:
        """Save node state atomically."""
        self._save_json(self.state_path, state_dict)

    def load_state(self) -> dict:
        """Load node state, returning an empty dict when absent."""
        return self._load_json(self.state_path)

    def save_kv(self, kv_dict: dict) -> None:
        """Save key-value data atomically."""
        self._save_json(self.kv_path, kv_dict)

    def load_kv(self) -> dict:
        """Load key-value data, returning an empty dict when absent."""
        return self._load_json(self.kv_path)

    def _save_json(self, path: Path, data: dict) -> None:
        temporary_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.data_dir,
                prefix=f"{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary_file:
                json.dump(data, temporary_file, ensure_ascii=False, indent=2)
                temporary_path = temporary_file.name
            os.replace(temporary_path, path)
        finally:
            if temporary_path and os.path.exists(temporary_path):
                os.remove(temporary_path)

    @staticmethod
    def _load_json(path: Path) -> dict:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
