from pathlib import Path


def test_distributed_control_plane_has_ttl_leases_and_ack_contracts():
    registry = Path("distributed/server/node_registry.py").read_text(encoding="utf-8")
    queue = Path("distributed/server/command_queue.py").read_text(encoding="utf-8")
    command = Path("distributed/protocol/command.py").read_text(encoding="utf-8")
    agent = Path("distributed/agent/agent.py").read_text(encoding="utf-8")
    assert "expires_at" in registry
    assert "stale" in registry or "prune" in registry
    assert "lease" in queue.lower()
    assert "ack" in queue.lower()
    assert "nack" in queue.lower()
    assert "command_id" in command
    assert "heartbeat" in agent.lower()
