"""Standard-library HTTP JSON RPC for Raft."""

import json
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class RPCClient:
    """HTTP JSON client used for Raft RPC calls."""

    def __init__(self, timeout: float = 0.5) -> None:
        self.timeout = timeout

    def request_vote(self, peer: str, request: dict) -> dict | None:
        return self._post(peer, "/raft/request_vote", request)

    def append_entries(self, peer: str, request: dict) -> dict | None:
        return self._post(peer, "/raft/append_entries", request)

    def install_snapshot(self, peer: str, request: dict) -> dict | None:
        return self._post(peer, "/raft/install_snapshot", request)

    def _post(self, peer: str, path: str, data: dict) -> dict | None:
        request = urllib.request.Request(
            f"http://{peer}{path}",
            data=json.dumps(data).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.load(response)
        except (OSError, urllib.error.URLError, json.JSONDecodeError):
            return None


class RPCServer:
    """HTTP server exposing Raft RPC and status endpoints."""

    def __init__(self, host: str, port: int, raft_node: Any) -> None:
        self.raft_node = raft_node
        handler = self._build_handler()
        self.http_server = ThreadingHTTPServer((host, port), handler)

    def serve_forever(self) -> None:
        """Serve requests until shutdown."""
        self.http_server.serve_forever()

    def shutdown(self) -> None:
        """Stop serving requests."""
        self.http_server.shutdown()
        self.http_server.server_close()

    def close(self) -> None:
        """Close the server after serve_forever has stopped."""
        self.http_server.server_close()

    def _build_handler(self) -> type[BaseHTTPRequestHandler]:
        raft_node = self.raft_node

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/status":
                    self._write_json(200, raft_node.status())
                elif self.path == "/cluster/members":
                    self._write_json(200, raft_node.cluster_members())
                elif self.path.startswith("/kv/"):
                    key = urllib.parse.unquote(self.path[len("/kv/") :])
                    response = raft_node.get_value(key)
                    if response.get("success"):
                        self._write_json(200, response)
                    elif response.get("error") == "NOT_FOUND":
                        self._write_json(404, response)
                    elif response.get("error") == "not leader":
                        self._write_json(409, response)
                    else:
                        self._write_json(503, response)
                else:
                    self._write_json(404, {"error": "not found"})

            def do_POST(self) -> None:
                try:
                    content_length = int(self.headers.get("Content-Length", "0"))
                    request = json.loads(self.rfile.read(content_length))
                    if self.path == "/raft/request_vote":
                        response = raft_node.handle_request_vote(request)
                    elif self.path == "/raft/append_entries":
                        response = raft_node.handle_append_entries(request)
                    elif self.path == "/raft/install_snapshot":
                        response = raft_node.handle_install_snapshot(request)
                    elif self.path == "/debug/append_log":
                        response = raft_node.append_command(request)
                    elif self.path == "/cluster/add_node":
                        response = raft_node.add_node(request)
                    elif self.path == "/cluster/remove_node":
                        response = raft_node.remove_node(request)
                    else:
                        self._write_json(404, {"error": "not found"})
                        return
                    if self.path.startswith("/cluster/"):
                        self._write_command_response(response)
                    else:
                        self._write_json(200, response)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                    self._write_json(400, {"error": str(error)})

            def do_PUT(self) -> None:
                if not self.path.startswith("/kv/"):
                    self._write_json(404, {"error": "not found"})
                    return
                try:
                    request = self._read_json()
                    key = urllib.parse.unquote(self.path[len("/kv/") :])
                    response = raft_node.append_command(
                        {"type": "put", "key": key, "value": request["value"]}
                    )
                    self._write_command_response(response)
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
                    self._write_json(400, {"error": str(error)})

            def do_DELETE(self) -> None:
                if not self.path.startswith("/kv/"):
                    self._write_json(404, {"error": "not found"})
                    return
                key = urllib.parse.unquote(self.path[len("/kv/") :])
                response = raft_node.append_command({"type": "delete", "key": key})
                self._write_command_response(response)

            def log_message(self, format: str, *args: object) -> None:
                return

            def _read_json(self) -> dict:
                content_length = int(self.headers.get("Content-Length", "0"))
                return json.loads(self.rfile.read(content_length))

            def _write_command_response(self, response: dict) -> None:
                if response.get("success"):
                    self._write_json(200, response)
                elif response.get("error") == "not leader":
                    response.update(raft_node.not_leader_response())
                    self._write_json(409, response)
                else:
                    self._write_json(503, response)

            def _write_json(self, status: int, data: dict) -> None:
                body = json.dumps(data).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler
