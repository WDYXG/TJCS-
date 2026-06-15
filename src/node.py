"""Command-line entry point for local KV and minimal Raft nodes."""

import argparse

from config import load_config
from raft import RaftNode
from rpc import RPCClient, RPCServer
from state_machine import KVStateMachine
from storage import JSONStorage


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser."""
    parser = argparse.ArgumentParser(
        description="Local KV tool and minimal Raft election server."
    )
    parser.add_argument("--node-id", required=True, help="Node identifier")
    parser.add_argument("--data-dir", help="Directory for local JSON data")
    parser.add_argument(
        "--config",
        default="config.json",
        help="Cluster JSON config; defaults are used when the file is absent",
    )

    commands = parser.add_subparsers(dest="command", required=True)

    put_parser = commands.add_parser("put", help="Store a key and value")
    put_parser.add_argument("key")
    put_parser.add_argument("value")

    get_parser = commands.add_parser("get", help="Read a key")
    get_parser.add_argument("key")

    delete_parser = commands.add_parser("delete", help="Delete a key")
    delete_parser.add_argument("key")

    commands.add_parser("status", help="Show local node status")
    commands.add_parser("serve", help="Start the minimal Raft HTTP server")
    return parser


def run_command(args: argparse.Namespace) -> None:
    """Load local state, execute one command, and persist changes."""
    if args.command == "serve":
        serve(args)
        return
    if not args.data_dir:
        raise SystemExit("--data-dir is required for local KV commands")

    storage = JSONStorage(args.data_dir)
    state_machine = KVStateMachine()
    state_machine.load(storage.load_kv())

    if args.command == "put":
        state_machine.put(args.key, args.value)
        storage.save_kv(state_machine.dump())
        print("OK")
    elif args.command == "get":
        value = state_machine.get(args.key)
        print("NOT_FOUND" if value is None else value)
    elif args.command == "delete":
        state_machine.delete(args.key)
        storage.save_kv(state_machine.dump())
        print("OK")
    elif args.command == "status":
        print(f"node_id: {args.node_id}")
        print(f"data_dir: {args.data_dir}")
        print(f"key_count: {len(state_machine.dump())}")


def serve(args: argparse.Namespace) -> None:
    """Start one minimal Raft node from cluster configuration."""
    cluster = load_config(args.config)
    node_config = next(
        (node for node in cluster.nodes if node.node_id == args.node_id),
        None,
    )
    if node_config is None:
        raise SystemExit(f"node_id not found in config: {args.node_id}")

    storage = JSONStorage(node_config.data_dir)
    state_machine = KVStateMachine()
    state_machine.load(storage.load_kv())
    node_urls = {
        node.node_id: f"http://{node.host}:{node.port}" for node in cluster.nodes
    }
    raft_node = RaftNode(
        args.node_id,
        node_config.peers,
        RPCClient(),
        state_machine=state_machine,
        storage=storage,
        node_urls=node_urls,
    )
    server = RPCServer(node_config.host, node_config.port, raft_node)
    raft_node.start()
    print(
        f"{args.node_id} serving on {node_config.host}:{node_config.port}",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        raft_node.stop()
        server.close()


def main() -> None:
    """Run one local KV command."""
    args = build_parser().parse_args()
    run_command(args)


if __name__ == "__main__":
    main()
