"""End-to-end tests for the single-node command-line tool."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
NODE_SCRIPT = PROJECT_ROOT / "src" / "node.py"


class NodeCLITest(unittest.TestCase):
    def run_cli(self, data_dir: str, *command: str) -> str:
        result = subprocess.run(
            [
                sys.executable,
                str(NODE_SCRIPT),
                "--node-id",
                "node1",
                "--data-dir",
                data_dir,
                *command,
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def test_put_is_restored_by_later_get(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            self.assertEqual(self.run_cli(data_dir, "put", "a", "1"), "OK")
            self.assertEqual(self.run_cli(data_dir, "get", "a"), "1")

    def test_delete_is_restored_by_later_get(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            self.run_cli(data_dir, "put", "a", "1")
            self.assertEqual(self.run_cli(data_dir, "delete", "a"), "OK")
            self.assertEqual(self.run_cli(data_dir, "get", "a"), "NOT_FOUND")

    def test_status_reports_node_and_key_count(self) -> None:
        with tempfile.TemporaryDirectory() as data_dir:
            self.run_cli(data_dir, "put", "a", "1")
            output = self.run_cli(data_dir, "status")
            self.assertIn("node_id: node1", output)
            self.assertIn(f"data_dir: {data_dir}", output)
            self.assertIn("key_count: 1", output)


if __name__ == "__main__":
    unittest.main()
