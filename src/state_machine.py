"""Single-node key-value state machine."""


class KVStateMachine:
    """In-memory key-value state machine."""

    def __init__(self) -> None:
        self._data: dict[str, object] = {}

    def put(self, key: str, value: object) -> bool:
        """Store a value and report success."""
        self._data[key] = value
        return True

    def get(self, key: str) -> object | None:
        """Return a value, or None when the key does not exist."""
        return self._data.get(key)

    def delete(self, key: str) -> bool:
        """Delete a key if it exists and report success."""
        self._data.pop(key, None)
        return True

    def apply(self, command: dict) -> object:
        """Apply a put or delete command."""
        command_type = command.get("type")
        if command_type == "put":
            return self.put(command["key"], command["value"])
        if command_type == "delete":
            return self.delete(command["key"])
        raise ValueError(f"Unsupported command type: {command_type}")

    def dump(self) -> dict:
        """Return a copy suitable for persistence."""
        return self._data.copy()

    def load(self, data: dict) -> None:
        """Replace current data with persisted data."""
        self._data = data.copy()
