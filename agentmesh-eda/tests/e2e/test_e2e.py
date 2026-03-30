import pytest
import asyncio
from nats.aio.client import Client as NATS
from agentmesh.mal.message import UniversalMessage


@pytest.mark.asyncio
@pytest.mark.integration
async def test_e2e_message_flow():
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    test_data = {"key": "value"}
    message = UniversalMessage(
        payload=test_data,
        routing={"targets": ["nats:test.subject"]},
    )

    # The test will publish a message and then subscribe to the same subject
    # to ensure the message is sent and received correctly.

    received_msg = None

    async def message_handler(msg):
        nonlocal received_msg
        received_msg = msg

    await nc.subscribe("test.subject", cb=message_handler)

    await nc.publish("test.subject", message.serialize())

    await asyncio.sleep(1)  # Wait for the message to be received

    assert received_msg is not None
    received_universal_message = UniversalMessage.deserialize(received_msg.data)
    assert received_universal_message.payload == test_data

    await nc.close()
