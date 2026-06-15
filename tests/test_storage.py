"""Tests for JSON file persistence."""

import tempfile
import unittest

from src.storage import JSONStorage


class JSONStorageTest(unittest.TestCase):
    def test_save_and_reload_data(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            storage = JSONStorage(data_dir)
            storage.save_state({"current_term": 2, "voted_for": "node2"})
            storage.save_kv({"a": "1"})
            storage.save_snapshot(
                {"last_included_index": 1, "last_included_term": 2, "kv": {"a": "1"}}
            )

            restored = JSONStorage(data_dir)
            self.assertEqual(
                restored.load_state(),
                {"current_term": 2, "voted_for": "node2"},
            )
            self.assertEqual(restored.load_kv(), {"a": "1"})
            self.assertEqual(restored.load_snapshot()["last_included_index"], 1)

    def test_missing_files_return_empty_dicts(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            storage = JSONStorage(data_dir)
            self.assertEqual(storage.load_state(), {})
            self.assertEqual(storage.load_kv(), {})
            self.assertEqual(storage.load_snapshot(), {})

    def test_creates_missing_data_directory(self) -> None:
        with tempfile.TemporaryDirectory() as parent_dir:
            data_dir = f"{parent_dir}/node1"
            storage = JSONStorage(data_dir)
            storage.save_kv({"a": "1"})
            self.assertEqual(storage.load_kv(), {"a": "1"})


if __name__ == "__main__":
    unittest.main()
