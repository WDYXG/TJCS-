"""Tests for the single-node key-value state machine."""

import unittest

from src.state_machine import KVStateMachine


class KVStateMachineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.state_machine = KVStateMachine()

    def test_put_and_get(self) -> None:
        self.assertTrue(self.state_machine.put("a", "1"))
        self.assertEqual(self.state_machine.get("a"), "1")

    def test_delete(self) -> None:
        self.state_machine.put("a", "1")
        self.assertTrue(self.state_machine.delete("a"))
        self.assertIsNone(self.state_machine.get("a"))

    def test_delete_missing_key(self) -> None:
        self.assertTrue(self.state_machine.delete("missing"))

    def test_apply_put_and_delete(self) -> None:
        self.assertTrue(
            self.state_machine.apply({"type": "put", "key": "a", "value": "1"})
        )
        self.assertEqual(self.state_machine.get("a"), "1")
        self.assertTrue(self.state_machine.apply({"type": "delete", "key": "a"}))
        self.assertIsNone(self.state_machine.get("a"))

    def test_dump_and_load(self) -> None:
        self.state_machine.put("a", "1")
        restored = KVStateMachine()
        restored.load(self.state_machine.dump())
        self.assertEqual(restored.get("a"), "1")


if __name__ == "__main__":
    unittest.main()
