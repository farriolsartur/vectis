"""Tests for Vectis Phase 3: In-Process Communication.

This module tests:
- Serializers (JSON, MessagePack)
- In-process channels (InProcessOutputChannel, InProcessInputChannel)
- Channel groups (FanOutChannelGroup, CompetingChannelGroup)
- ChannelFactory
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import pytest

from vectis import Message, MessageType
from vectis.communication import (
    ChannelFactory,
    CompetingChannelGroup,
    CompetingStrategy,
    DistributionMode,
    FanOutChannelGroup,
    InProcessInputChannel,
    InProcessOutputChannel,
    JSONSerializer,
    TransportType,
    get_serializer,
)


# Helper to create channel pairs via factory
def _create_channel_pair(
    queue_size: int = 0, name: str | None = None
) -> tuple[InProcessOutputChannel, InProcessInputChannel]:
    """Create a channel pair using the factory."""
    factory = ChannelFactory()
    return factory.create_inprocess_pair(queue_size=queue_size, name=name)
from vectis.exceptions import ChannelClosedError


# =============================================================================
# Serializer Tests
# =============================================================================


class TestJSONSerializer:
    """Tests for JSONSerializer."""

    def test_serialize_returns_bytes(self, sample_data_message: Message[Any]) -> None:
        """Serialize should return bytes."""
        serializer = JSONSerializer()
        result = serializer.serialize(sample_data_message)
        assert isinstance(result, bytes)

    def test_deserialize_returns_message(
        self, sample_data_message: Message[Any]
    ) -> None:
        """Deserialize should return a Message."""
        serializer = JSONSerializer()
        data = serializer.serialize(sample_data_message)
        result = serializer.deserialize(data)
        assert isinstance(result, Message)

    def test_roundtrip_preserves_data(self, sample_data_message: Message[Any]) -> None:
        """Serialize/deserialize should preserve message data."""
        serializer = JSONSerializer()
        data = serializer.serialize(sample_data_message)
        restored = serializer.deserialize(data)

        assert restored.id == sample_data_message.id
        assert restored.message_type == sample_data_message.message_type
        assert restored.payload == sample_data_message.payload
        assert restored.source_component == sample_data_message.source_component

    def test_roundtrip_preserves_error_message(
        self, sample_error_message: Message[str]
    ) -> None:
        """Serialize/deserialize should preserve error messages."""
        serializer = JSONSerializer()
        data = serializer.serialize(sample_error_message)
        restored = serializer.deserialize(data)

        assert restored.id == sample_error_message.id
        assert restored.message_type == MessageType.ERROR
        assert restored.payload == sample_error_message.payload

    def test_roundtrip_preserves_eos_message(
        self, sample_eos_message: Message[None]
    ) -> None:
        """Serialize/deserialize should preserve EOS messages."""
        serializer = JSONSerializer()
        data = serializer.serialize(sample_eos_message)
        restored = serializer.deserialize(data)

        assert restored.id == sample_eos_message.id
        assert restored.message_type == MessageType.END_OF_STREAM
        assert restored.payload is None

    def test_serialize_with_indent(self) -> None:
        """Serializer should support indentation option."""
        serializer = JSONSerializer(indent=2)
        msg = Message.data(payload={"test": 1}, source_component="test")
        data = serializer.serialize(msg)
        # Indented JSON will have newlines
        assert b"\n" in data

    def test_serialize_compact_by_default(self) -> None:
        """Serializer should be compact by default."""
        serializer = JSONSerializer()
        msg = Message.data(payload={"test": 1}, source_component="test")
        data = serializer.serialize(msg)
        # Compact JSON has no newlines
        assert b"\n" not in data

    def test_deserialize_invalid_json_raises(self) -> None:
        """Deserialize should raise on invalid JSON."""
        serializer = JSONSerializer()
        with pytest.raises(json.JSONDecodeError):
            serializer.deserialize(b"not valid json")

    def test_deserialize_invalid_schema_raises(self) -> None:
        """Deserialize should raise on invalid message schema."""
        serializer = JSONSerializer()
        with pytest.raises(Exception):  # pydantic.ValidationError
            serializer.deserialize(b'{"not": "a message"}')


class TestMessagePackSerializer:
    """Tests for MessagePackSerializer."""

    @staticmethod
    def _msgpack_available() -> bool:
        """Check if msgpack is available."""
        try:
            import msgpack

            return True
        except ImportError:
            return False

    @pytest.mark.skipif(
        not _msgpack_available.__func__(),
        reason="msgpack not installed",
    )
    def test_serialize_returns_bytes(self, sample_data_message: Message[Any]) -> None:
        """Serialize should return bytes."""
        from vectis.communication import MessagePackSerializer

        serializer = MessagePackSerializer()
        result = serializer.serialize(sample_data_message)
        assert isinstance(result, bytes)

    @pytest.mark.skipif(
        not _msgpack_available.__func__(),
        reason="msgpack not installed",
    )
    def test_roundtrip_preserves_data(self, sample_data_message: Message[Any]) -> None:
        """Serialize/deserialize should preserve message data."""
        from vectis.communication import MessagePackSerializer

        serializer = MessagePackSerializer()
        data = serializer.serialize(sample_data_message)
        restored = serializer.deserialize(data)

        assert restored.id == sample_data_message.id
        assert restored.payload == sample_data_message.payload

    @pytest.mark.skipif(
        not _msgpack_available.__func__(),
        reason="msgpack not installed",
    )
    def test_more_compact_than_json(self, sample_data_message: Message[Any]) -> None:
        """MessagePack should be more compact than JSON."""
        from vectis.communication import MessagePackSerializer

        json_serializer = JSONSerializer()
        msgpack_serializer = MessagePackSerializer()

        json_data = json_serializer.serialize(sample_data_message)
        msgpack_data = msgpack_serializer.serialize(sample_data_message)

        assert len(msgpack_data) < len(json_data)


class TestGetSerializer:
    """Tests for get_serializer helper function."""

    def test_get_json_serializer(self) -> None:
        """get_serializer('json') should return JSONSerializer."""
        serializer = get_serializer("json")
        assert isinstance(serializer, JSONSerializer)

    def test_get_unknown_serializer_raises(self) -> None:
        """get_serializer should raise for unknown serializers."""
        with pytest.raises(ValueError, match="Unknown serializer"):
            get_serializer("unknown")


# =============================================================================
# In-Process Channel Tests
# =============================================================================


class TestInProcessChannels:
    """Tests for in-process channels."""

    @pytest.mark.asyncio
    async def test_send_and_receive_message(self) -> None:
        """Messages should flow from output to input channel."""
        output, input_ch = _create_channel_pair()
        msg = Message.data(payload={"test": 42}, source_component="test")

        await output.send(msg)
        received = await input_ch.receive()

        assert received.id == msg.id
        assert received.payload == {"test": 42}

    @pytest.mark.asyncio
    async def test_fifo_order_preserved(self) -> None:
        """Messages should be received in FIFO order."""
        output, input_ch = _create_channel_pair()

        for i in range(5):
            msg = Message.data(payload={"index": i}, source_component="test")
            await output.send(msg)

        for i in range(5):
            received = await input_ch.receive()
            assert received.payload["index"] == i

    @pytest.mark.asyncio
    async def test_send_on_closed_channel_raises(self) -> None:
        """Sending on closed channel should raise ChannelClosedError."""
        output, _ = _create_channel_pair()
        await output.close()

        msg = Message.data(payload={}, source_component="test")
        with pytest.raises(ChannelClosedError):
            await output.send(msg)

    @pytest.mark.asyncio
    async def test_receive_on_closed_channel_raises(self) -> None:
        """Receiving on closed channel should raise ChannelClosedError."""
        _, input_ch = _create_channel_pair()
        await input_ch.close()

        with pytest.raises(ChannelClosedError):
            await input_ch.receive()

    @pytest.mark.asyncio
    async def test_bounded_queue_blocks_when_full(self) -> None:
        """Bounded queue should block send when full."""
        output, input_ch = _create_channel_pair(queue_size=1)

        msg1 = Message.data(payload={"n": 1}, source_component="test")
        msg2 = Message.data(payload={"n": 2}, source_component="test")

        await output.send(msg1)  # Should succeed

        # Second send should block (queue full)
        send_task = asyncio.create_task(output.send(msg2))
        await asyncio.sleep(0.01)  # Give it time to start
        assert not send_task.done()  # Should still be waiting

        # Receive to unblock
        await input_ch.receive()
        await asyncio.wait_for(send_task, timeout=1.0)  # Should complete now

    @pytest.mark.asyncio
    async def test_set_handler_stores_callback(self) -> None:
        """set_handler should store the callback."""
        _, input_ch = _create_channel_pair()

        async def my_handler(msg: Message[Any]) -> None:
            pass

        input_ch.set_handler(my_handler)
        assert input_ch.handler is my_handler

    @pytest.mark.asyncio
    async def test_multiple_messages_different_types(self) -> None:
        """Channel should handle all message types."""
        output, input_ch = _create_channel_pair()

        data_msg = Message.data(payload={"x": 1}, source_component="test")
        error_msg = Message.error(error="fail", source_component="test")
        eos_msg = Message.end_of_stream(source_component="test")

        await output.send(data_msg)
        await output.send(error_msg)
        await output.send(eos_msg)

        r1 = await input_ch.receive()
        r2 = await input_ch.receive()
        r3 = await input_ch.receive()

        assert r1.is_data
        assert r2.is_error
        assert r3.is_end_of_stream

    @pytest.mark.asyncio
    async def test_channel_name_in_error(self) -> None:
        """ChannelClosedError should include channel name."""
        output, _ = _create_channel_pair(name="test-channel")
        await output.close()

        with pytest.raises(ChannelClosedError) as exc_info:
            await output.send(Message.data(payload={}, source_component="test"))

        assert "test-channel" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_is_closed_property(self) -> None:
        """is_closed property should reflect channel state."""
        output, input_ch = _create_channel_pair()

        assert not output.is_closed
        assert not input_ch.is_closed

        await output.close()
        await input_ch.close()

        assert output.is_closed
        assert input_ch.is_closed

    @pytest.mark.asyncio
    async def test_name_property(self) -> None:
        """name property should return channel name."""
        output, input_ch = _create_channel_pair(name="my-channel")

        assert "my-channel" in output.name
        assert "my-channel" in input_ch.name


# =============================================================================
# Fan-Out Channel Group Tests
# =============================================================================


class TestFanOutChannelGroup:
    """Tests for FanOutChannelGroup."""

    @pytest.mark.asyncio
    async def test_send_to_all_channels(self) -> None:
        """Messages should be sent to all channels."""
        # Create 3 channel pairs
        pairs = [_create_channel_pair() for _ in range(3)]
        outputs = [p[0] for p in pairs]
        inputs = [p[1] for p in pairs]

        group = FanOutChannelGroup(channels=outputs)
        msg = Message.data(payload={"value": 42}, source_component="test")

        await group.send(msg)

        # All inputs should receive the message
        for input_ch in inputs:
            received = await input_ch.receive()
            assert received.id == msg.id
            assert received.payload == {"value": 42}

    @pytest.mark.asyncio
    async def test_error_sent_to_all(self) -> None:
        """ERROR messages should be sent to all channels."""
        pairs = [_create_channel_pair() for _ in range(2)]
        outputs = [p[0] for p in pairs]
        inputs = [p[1] for p in pairs]

        group = FanOutChannelGroup(channels=outputs)
        msg = Message.error(error="test error", source_component="test")

        await group.send(msg)

        for input_ch in inputs:
            received = await input_ch.receive()
            assert received.is_error

    @pytest.mark.asyncio
    async def test_eos_sent_to_all(self) -> None:
        """END_OF_STREAM messages should be sent to all channels."""
        pairs = [_create_channel_pair() for _ in range(2)]
        outputs = [p[0] for p in pairs]
        inputs = [p[1] for p in pairs]

        group = FanOutChannelGroup(channels=outputs)
        msg = Message.end_of_stream(source_component="test")

        await group.send(msg)

        for input_ch in inputs:
            received = await input_ch.receive()
            assert received.is_end_of_stream

    @pytest.mark.asyncio
    async def test_close_closes_all_channels(self) -> None:
        """close() should close all channels in the group."""
        pairs = [_create_channel_pair() for _ in range(2)]
        outputs = [p[0] for p in pairs]

        group = FanOutChannelGroup(channels=outputs)
        await group.close()

        for output in outputs:
            assert output.is_closed

    @pytest.mark.asyncio
    async def test_add_channel(self) -> None:
        """add_channel should add a channel to the group."""
        group = FanOutChannelGroup()
        assert group.channel_count == 0

        output, _ = _create_channel_pair()
        group.add_channel(output)

        assert group.channel_count == 1

    @pytest.mark.asyncio
    async def test_send_with_no_channels_warns(self, caplog: Any) -> None:
        """Sending with no channels should log a warning."""
        group = FanOutChannelGroup()
        msg = Message.data(payload={}, source_component="test")

        with caplog.at_level(logging.WARNING):
            await group.send(msg)

        assert "no channels" in caplog.text.lower()

    @pytest.mark.asyncio
    async def test_channel_count_property(self) -> None:
        """channel_count property should return correct count."""
        pairs = [_create_channel_pair() for _ in range(3)]
        outputs = [p[0] for p in pairs]

        group = FanOutChannelGroup(channels=outputs)
        assert group.channel_count == 3


# =============================================================================
# Competing Channel Group Tests
# =============================================================================


class TestCompetingChannelGroup:
    """Tests for CompetingChannelGroup."""

    @pytest.mark.asyncio
    async def test_data_sent_to_one_channel_round_robin(self) -> None:
        """DATA should go to one channel at a time (round-robin)."""
        pairs = [_create_channel_pair() for _ in range(3)]
        outputs = [p[0] for p in pairs]
        inputs = [p[1] for p in pairs]

        group = CompetingChannelGroup(
            channels=outputs,
            strategy=CompetingStrategy.ROUND_ROBIN,
        )

        # Send 6 messages
        for i in range(6):
            msg = Message.data(payload={"n": i}, source_component="test")
            await group.send(msg)

        # Each channel should have received 2 messages
        for input_ch in inputs:
            count = 0
            while True:
                try:
                    await asyncio.wait_for(input_ch.receive(), timeout=0.01)
                    count += 1
                except asyncio.TimeoutError:
                    break
            assert count == 2

    @pytest.mark.asyncio
    async def test_data_distributed_random(self) -> None:
        """DATA should be distributed (randomly) with RANDOM strategy."""
        pairs = [_create_channel_pair() for _ in range(2)]
        outputs = [p[0] for p in pairs]
        inputs = [p[1] for p in pairs]

        group = CompetingChannelGroup(
            channels=outputs,
            strategy=CompetingStrategy.RANDOM,
        )

        # Send 10 messages
        for i in range(10):
            msg = Message.data(payload={"n": i}, source_component="test")
            await group.send(msg)

        # Count messages per channel
        counts = []
        for input_ch in inputs:
            count = 0
            while True:
                try:
                    await asyncio.wait_for(input_ch.receive(), timeout=0.01)
                    count += 1
                except asyncio.TimeoutError:
                    break
            counts.append(count)

        assert sum(counts) == 10  # Total must be 10

    @pytest.mark.asyncio
    async def test_error_sent_to_all_channels(self) -> None:
        """ERROR should be sent to ALL channels."""
        pairs = [_create_channel_pair() for _ in range(3)]
        outputs = [p[0] for p in pairs]
        inputs = [p[1] for p in pairs]

        group = CompetingChannelGroup(channels=outputs)
        msg = Message.error(error="test error", source_component="test")

        await group.send(msg)

        # All channels should receive the error
        for input_ch in inputs:
            received = await input_ch.receive()
            assert received.is_error

    @pytest.mark.asyncio
    async def test_eos_sent_to_all_channels(self) -> None:
        """END_OF_STREAM should be sent to ALL channels."""
        pairs = [_create_channel_pair() for _ in range(3)]
        outputs = [p[0] for p in pairs]
        inputs = [p[1] for p in pairs]

        group = CompetingChannelGroup(channels=outputs)
        msg = Message.end_of_stream(source_component="test")

        await group.send(msg)

        # All channels should receive EOS
        for input_ch in inputs:
            received = await input_ch.receive()
            assert received.is_end_of_stream

    @pytest.mark.asyncio
    async def test_close_closes_all_channels(self) -> None:
        """close() should close all channels."""
        pairs = [_create_channel_pair() for _ in range(2)]
        outputs = [p[0] for p in pairs]

        group = CompetingChannelGroup(channels=outputs)
        await group.close()

        for output in outputs:
            assert output.is_closed

    @pytest.mark.asyncio
    async def test_strategy_property(self) -> None:
        """strategy property should return the current strategy."""
        group = CompetingChannelGroup(strategy=CompetingStrategy.RANDOM)
        assert group.strategy == CompetingStrategy.RANDOM

    @pytest.mark.asyncio
    async def test_send_with_no_channels_warns(self, caplog: Any) -> None:
        """Sending with no channels should log a warning."""
        group = CompetingChannelGroup()
        msg = Message.data(payload={}, source_component="test")

        with caplog.at_level(logging.WARNING):
            await group.send(msg)

        assert "no channels" in caplog.text.lower()


# =============================================================================
# Channel Factory Tests
# =============================================================================


class TestChannelFactory:
    """Tests for ChannelFactory."""

    def test_create_inprocess_pair(self) -> None:
        """Should create in-process channel pair."""
        factory = ChannelFactory()
        output, input_ch = factory.create_inprocess_pair(queue_size=10)

        assert isinstance(output, InProcessOutputChannel)
        assert isinstance(input_ch, InProcessInputChannel)

    def test_create_channel_group_fanout(self) -> None:
        """Should create fan-out channel group."""
        factory = ChannelFactory()
        group = factory.create_channel_group(DistributionMode.FAN_OUT)

        assert isinstance(group, FanOutChannelGroup)

    def test_create_channel_group_competing(self) -> None:
        """Should create competing channel group."""
        factory = ChannelFactory()
        group = factory.create_channel_group(
            DistributionMode.COMPETING,
            strategy=CompetingStrategy.RANDOM,
        )

        assert isinstance(group, CompetingChannelGroup)
        assert group.strategy == CompetingStrategy.RANDOM

    @pytest.mark.asyncio
    async def test_created_channels_communicate(self) -> None:
        """Created channels should be able to communicate."""
        factory = ChannelFactory()
        output, input_ch = factory.create_inprocess_pair()

        msg = Message.data(payload={"x": 1}, source_component="test")
        await output.send(msg)
        received = await input_ch.receive()

        assert received.payload == {"x": 1}

    @pytest.mark.asyncio
    async def test_create_output_channel_requires_queue(self) -> None:
        """create_output_channel should require queue for INPROCESS."""
        factory = ChannelFactory()
        with pytest.raises(ValueError, match="queue is required"):
            factory.create_output_channel(TransportType.INPROCESS)

    @pytest.mark.asyncio
    async def test_create_input_channel_requires_queue(self) -> None:
        """create_input_channel should require queue for INPROCESS."""
        factory = ChannelFactory()
        with pytest.raises(ValueError, match="queue is required"):
            factory.create_input_channel(TransportType.INPROCESS)

    @pytest.mark.asyncio
    async def test_create_channels_with_shared_queue(self) -> None:
        """Creating channels with same queue should allow communication."""
        import asyncio

        factory = ChannelFactory()
        queue: asyncio.Queue[Any] = asyncio.Queue()

        output = factory.create_output_channel(
            TransportType.INPROCESS, queue=queue
        )
        input_ch = factory.create_input_channel(
            TransportType.INPROCESS, queue=queue
        )

        msg = Message.data(payload={"shared": True}, source_component="test")
        await output.send(msg)
        received = await input_ch.receive()

        assert received.payload == {"shared": True}


# =============================================================================
# Integration Tests
# =============================================================================


class TestChannelIntegration:
    """Integration tests for channels and groups."""

    @pytest.mark.asyncio
    async def test_data_provider_to_algorithm_via_channels(self) -> None:
        """Test message flow: DataProvider -> Channel -> Algorithm."""
        # Create channel pair
        output, input_ch = _create_channel_pair()

        # Simulate DataProvider sending
        for i in range(3):
            msg = Message.data(payload={"count": i}, source_component="provider")
            await output.send(msg)
        eos = Message.end_of_stream(source_component="provider")
        await output.send(eos)

        # Simulate Algorithm receiving
        received = []
        while True:
            msg = await input_ch.receive()
            if msg.is_end_of_stream:
                break
            received.append(msg.payload)

        assert received == [{"count": 0}, {"count": 1}, {"count": 2}]

    @pytest.mark.asyncio
    async def test_fanout_to_multiple_algorithms(self) -> None:
        """Test fan-out: 1 provider -> multiple algorithms."""
        # Create 3 channel pairs for 3 "algorithms"
        pairs = [_create_channel_pair() for _ in range(3)]
        outputs = [p[0] for p in pairs]
        inputs = [p[1] for p in pairs]

        # Create fan-out group
        group = FanOutChannelGroup(channels=outputs)

        # Provider sends 2 data + EOS
        await group.send(Message.data(payload={"n": 1}, source_component="p"))
        await group.send(Message.data(payload={"n": 2}, source_component="p"))
        await group.send(Message.end_of_stream(source_component="p"))

        # Each algorithm should receive all messages
        for input_ch in inputs:
            m1 = await input_ch.receive()
            m2 = await input_ch.receive()
            m3 = await input_ch.receive()

            assert m1.payload == {"n": 1}
            assert m2.payload == {"n": 2}
            assert m3.is_end_of_stream

    @pytest.mark.asyncio
    async def test_competing_load_balance(self) -> None:
        """Test competing: load balances DATA across algorithms."""
        pairs = [_create_channel_pair() for _ in range(2)]
        outputs = [p[0] for p in pairs]
        inputs = [p[1] for p in pairs]

        group = CompetingChannelGroup(
            channels=outputs,
            strategy=CompetingStrategy.ROUND_ROBIN,
        )

        # Send 4 data messages (should alternate between 2 channels)
        for i in range(4):
            await group.send(Message.data(payload={"n": i}, source_component="p"))
        # Send EOS (goes to all)
        await group.send(Message.end_of_stream(source_component="p"))

        # Channel 0 should have: n=0, n=2, EOS
        # Channel 1 should have: n=1, n=3, EOS
        r0_1 = await inputs[0].receive()
        r0_2 = await inputs[0].receive()
        r0_3 = await inputs[0].receive()

        r1_1 = await inputs[1].receive()
        r1_2 = await inputs[1].receive()
        r1_3 = await inputs[1].receive()

        assert r0_1.payload == {"n": 0}
        assert r0_2.payload == {"n": 2}
        assert r0_3.is_end_of_stream

        assert r1_1.payload == {"n": 1}
        assert r1_2.payload == {"n": 3}
        assert r1_3.is_end_of_stream

    @pytest.mark.asyncio
    async def test_serializer_with_channel(self) -> None:
        """Test that serialized messages can flow through channels."""
        serializer = JSONSerializer()
        output, input_ch = _create_channel_pair()

        # Send original message
        original = Message.data(payload={"key": "value"}, source_component="sender")
        await output.send(original)

        # Receive and verify
        received = await input_ch.receive()

        # Serialize and deserialize
        data = serializer.serialize(received)
        restored = serializer.deserialize(data)

        assert restored.payload == original.payload
        assert restored.source_component == original.source_component

    @pytest.mark.asyncio
    async def test_factory_creates_working_pipeline(self) -> None:
        """Test that factory-created components work together."""
        factory = ChannelFactory()

        # Create channel pairs
        pair1 = factory.create_inprocess_pair(name="ch1")
        pair2 = factory.create_inprocess_pair(name="ch2")

        # Create fan-out group
        group = factory.create_channel_group(
            DistributionMode.FAN_OUT,
            channels=[pair1[0], pair2[0]],
        )

        # Send a message
        msg = Message.data(payload={"factory": "test"}, source_component="test")
        await group.send(msg)

        # Both channels should receive it
        r1 = await pair1[1].receive()
        r2 = await pair2[1].receive()

        assert r1.payload == {"factory": "test"}
        assert r2.payload == {"factory": "test"}
