from distributed.protocol import PROTOCOL_VERSIONS
from distributed.protocol.ack import CommandAck
from distributed.protocol.command import RuntimeCommand
from distributed.protocol.event import RuntimeEvent
from distributed.protocol.heartbeat import NodeHeartbeat


def test_distributed_protocol_contracts():
    assert PROTOCOL_VERSIONS["event"] == "cajeer.bots.event.v1"
    event = RuntimeEvent(node_id="node-1", bot_id="bot-1", platform="telegram", type="message.received")
    command = RuntimeCommand(node_id="node-1", bot_id="bot-1", type="message.send")
    ack = CommandAck(command_id=command.command_id, status="success")
    heartbeat = NodeHeartbeat(node_id="node-1", status="online", bots_running=1)
    assert event.to_dict()["schema"] == PROTOCOL_VERSIONS["event"]
    assert command.to_dict()["schema"] == PROTOCOL_VERSIONS["command"]
    assert ack.to_dict()["schema"] == PROTOCOL_VERSIONS["ack"]
    assert heartbeat.to_dict()["schema"] == PROTOCOL_VERSIONS["heartbeat"]
