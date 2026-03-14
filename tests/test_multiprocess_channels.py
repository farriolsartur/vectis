"""Tests for Vectis multiprocess channels."""

from __future__ import annotations

import asyncio
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from vectis import Message
from vectis.communication.channels.multiprocess import (
    AsyncQueueWrapper,
    MultiprocessInputChannel,
    MultiprocessOutputChannel,
)
from vectis.communication.enums import BackpressureMode
from vectis.communication.serialization.json_serializer import JSONSerializer
from vectis.exceptions import BackpressureDroppedError, ChannelClosedError


# =============================================================================
# TestAsyncQueueWrapper
# =============================================================================


class TestAsyncQueueWrapper:
    """Tests for AsyncQueueWrapper."""

    @pytest.mark.asyncio
    async def test_put_and_get_item(self):
        """Basic roundtrip."""
        queue: multiprocessing.Queue[str] = multiprocessing.Queue()
        wrapper = AsyncQueueWrapper(queue)

        try:
            await wrapper.put("test_item")
            result = await wrapper.get()
            assert result == "test_item"
        finally:
            wrapper.close()

    @pytest.mark.asyncio
    async def test_put_nowait_success(self):
        """Returns True on empty queue."""
        queue: multiprocessing.Queue[str] = multiprocessing.Queue(maxsize=10)
        wrapper = AsyncQueueWrapper(queue)

        try:
            result = await wrapper.put_nowait("test_item")
            assert result is True
        finally:
            wrapper.close()

    @pytest.mark.asyncio
    async def test_put_nowait_full_queue(self):
        """Returns False when full."""
        queue: multiprocessing.Queue[str] = multiprocessing.Queue(maxsize=1)
        wrapper = AsyncQueueWrapper(queue)

        try:
            # Fill the queue
            await wrapper.put_nowait("item1")
            # Now it should be full
            result = await wrapper.put_nowait("item2")
            assert result is False
        finally:
            wrapper.close()

    @pytest.mark.asyncio
    async def test_full_returns_queue_state(self):
        """Accurate full() check."""
        queue: multiprocessing.Queue[str] = multiprocessing.Queue(maxsize=1)
        wrapper = AsyncQueueWrapper(queue)

        try:
            assert wrapper.full() is False
            await wrapper.put_nowait("item1")
            assert wrapper.full() is True
        finally:
            wrapper.close()

    @pytest.mark.asyncio
    async def test_close_shuts_down_owned_executor(self):
        """Cleanup when owning."""
        queue: multiprocessing.Queue[str] = multiprocessing.Queue()
        wrapper = AsyncQueueWrapper(queue)

        # When we own the executor, it should be shut down on close
        assert wrapper._owns_executor is True
        wrapper.close()
        # Executor should be shut down
        assert wrapper._executor._shutdown is True

    @pytest.mark.asyncio
    async def test_provided_executor_not_shut_down(self):
        """Does not shut down provided executor."""
        queue: multiprocessing.Queue[str] = multiprocessing.Queue()
        executor = ThreadPoolExecutor(max_workers=1)

        try:
            wrapper = AsyncQueueWrapper(queue, executor=executor)
            assert wrapper._owns_executor is False
            wrapper.close()
            # Executor should NOT be shut down
            assert executor._shutdown is False
        finally:
            executor.shutdown(wait=False)


# =============================================================================
# TestMultiprocessOutputChannel
# =============================================================================


class TestMultiprocessOutputChannel:
    """Tests for MultiprocessOutputChannel."""

    @pytest.mark.asyncio
    async def test_send_serializes_message(self, json_serializer):
        """Serializer called."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()
        channel = MultiprocessOutputChannel(
            queue=queue,
            serializer=json_serializer,
            name="test-output",
        )

        message = Message.data(payload={"value": 42}, source_component="test")
        await channel.send(message)

        # Verify data was serialized and placed in queue
        raw_data = queue.get(timeout=1.0)
        assert isinstance(raw_data, bytes)

        # Deserialize and verify
        restored = json_serializer.deserialize(raw_data)
        assert restored.payload == {"value": 42}

        await channel.close()

    @pytest.mark.asyncio
    async def test_send_on_closed_channel_raises(self, json_serializer):
        """ChannelClosedError raised."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()
        channel = MultiprocessOutputChannel(
            queue=queue,
            serializer=json_serializer,
            name="test-output",
        )

        await channel.close()

        message = Message.data(payload={"value": 1}, source_component="test")
        with pytest.raises(ChannelClosedError):
            await channel.send(message)

    @pytest.mark.asyncio
    async def test_send_with_block_mode_waits(self, json_serializer):
        """BLOCK backpressure blocks."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue(maxsize=1)
        channel = MultiprocessOutputChannel(
            queue=queue,
            serializer=json_serializer,
            name="test-output",
            backpressure_mode=BackpressureMode.BLOCK,
        )

        msg1 = Message.data(payload={"value": 1}, source_component="test")
        msg2 = Message.data(payload={"value": 2}, source_component="test")

        # First message should succeed
        await channel.send(msg1)

        # Second message with block mode should eventually succeed if we drain
        async def drain_and_send():
            await asyncio.sleep(0.05)
            queue.get()  # Drain the queue

        async def send_blocked():
            await channel.send(msg2)

        # Run both concurrently
        await asyncio.gather(drain_and_send(), send_blocked())

        await channel.close()

    @pytest.mark.asyncio
    async def test_send_with_drop_mode_raises_on_full(self, json_serializer):
        """BackpressureDroppedError when full."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue(maxsize=1)
        channel = MultiprocessOutputChannel(
            queue=queue,
            serializer=json_serializer,
            name="test-output",
            backpressure_mode=BackpressureMode.DROP,
        )

        msg1 = Message.data(payload={"value": 1}, source_component="test")
        msg2 = Message.data(payload={"value": 2}, source_component="test")

        await channel.send(msg1)

        with pytest.raises(BackpressureDroppedError):
            await channel.send(msg2)

        await channel.close()

    @pytest.mark.asyncio
    async def test_send_with_drop_mode_succeeds_when_space(self, json_serializer):
        """No error with space."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue(maxsize=10)
        channel = MultiprocessOutputChannel(
            queue=queue,
            serializer=json_serializer,
            name="test-output",
            backpressure_mode=BackpressureMode.DROP,
        )

        msg = Message.data(payload={"value": 1}, source_component="test")
        # Should not raise
        await channel.send(msg)

        await channel.close()

    @pytest.mark.asyncio
    async def test_close_sets_closed_flag(self, json_serializer):
        """is_closed becomes True."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()
        channel = MultiprocessOutputChannel(
            queue=queue,
            serializer=json_serializer,
        )

        assert channel.is_closed is False
        await channel.close()
        assert channel.is_closed is True

    def test_name_property(self, json_serializer):
        """Returns configured name."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()
        channel = MultiprocessOutputChannel(
            queue=queue,
            serializer=json_serializer,
            name="my-channel",
        )

        assert channel.name == "my-channel"


# =============================================================================
# TestMultiprocessInputChannel
# =============================================================================


class TestMultiprocessInputChannel:
    """Tests for MultiprocessInputChannel."""

    @pytest.mark.asyncio
    async def test_receive_deserializes_message(self, json_serializer):
        """Returns Message object."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()
        channel = MultiprocessInputChannel(
            queue=queue,
            serializer=json_serializer,
            name="test-input",
        )

        # Put serialized message directly in queue
        msg = Message.data(payload={"value": 42}, source_component="test")
        queue.put(json_serializer.serialize(msg))

        result = await channel.receive()
        assert isinstance(result, Message)
        assert result.payload == {"value": 42}
        assert result.source_component == "test"

        await channel.close()

    @pytest.mark.asyncio
    async def test_receive_on_closed_channel_raises(self, json_serializer):
        """ChannelClosedError raised."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()
        channel = MultiprocessInputChannel(
            queue=queue,
            serializer=json_serializer,
        )

        await channel.close()

        with pytest.raises(ChannelClosedError):
            await channel.receive()

    @pytest.mark.asyncio
    async def test_set_handler_stores_callback(self, json_serializer):
        """Handler stored."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()
        channel = MultiprocessInputChannel(
            queue=queue,
            serializer=json_serializer,
        )

        async def my_handler(message: Message[Any]) -> None:
            pass

        assert channel.handler is None
        channel.set_handler(my_handler)
        assert channel.handler is my_handler

        await channel.close()

    @pytest.mark.asyncio
    async def test_handler_property(self, json_serializer):
        """Returns current handler."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()
        channel = MultiprocessInputChannel(
            queue=queue,
            serializer=json_serializer,
        )

        async def handler1(message: Message[Any]) -> None:
            pass

        async def handler2(message: Message[Any]) -> None:
            pass

        channel.set_handler(handler1)
        assert channel.handler is handler1

        channel.set_handler(handler2)
        assert channel.handler is handler2

        await channel.close()


# =============================================================================
# TestMultiprocessChannelIntegration
# =============================================================================


class TestMultiprocessChannelIntegration:
    """Integration tests for multiprocess channels."""

    @pytest.mark.asyncio
    async def test_send_receive_roundtrip(self, json_serializer):
        """End-to-end flow."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()

        output_channel = MultiprocessOutputChannel(
            queue=queue,
            serializer=json_serializer,
            name="test-output",
        )

        input_channel = MultiprocessInputChannel(
            queue=queue,
            serializer=json_serializer,
            name="test-input",
        )

        msg = Message.data(payload={"key": "value", "num": 123}, source_component="producer")
        await output_channel.send(msg)

        received = await input_channel.receive()
        assert received.payload == {"key": "value", "num": 123}
        assert received.source_component == "producer"

        await output_channel.close()
        await input_channel.close()

    @pytest.mark.asyncio
    async def test_multiple_messages_fifo(self, json_serializer):
        """Order preserved."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()

        output_channel = MultiprocessOutputChannel(
            queue=queue,
            serializer=json_serializer,
        )

        input_channel = MultiprocessInputChannel(
            queue=queue,
            serializer=json_serializer,
        )

        # Send multiple messages
        for i in range(5):
            msg = Message.data(payload={"index": i}, source_component="test")
            await output_channel.send(msg)

        # Receive and verify order
        for i in range(5):
            received = await input_channel.receive()
            assert received.payload == {"index": i}

        await output_channel.close()
        await input_channel.close()

    @pytest.mark.asyncio
    async def test_different_message_types(self, json_serializer):
        """DATA/ERROR/EOS work."""
        queue: multiprocessing.Queue[bytes] = multiprocessing.Queue()

        output_channel = MultiprocessOutputChannel(
            queue=queue,
            serializer=json_serializer,
        )

        input_channel = MultiprocessInputChannel(
            queue=queue,
            serializer=json_serializer,
        )

        # Send different message types
        data_msg = Message.data(payload={"value": 1}, source_component="test")
        error_msg = Message.error(error="Test error", source_component="test")
        eos_msg = Message.end_of_stream(source_component="test")

        await output_channel.send(data_msg)
        await output_channel.send(error_msg)
        await output_channel.send(eos_msg)

        # Receive and verify types
        from vectis import MessageType

        received_data = await input_channel.receive()
        assert received_data.message_type == MessageType.DATA

        received_error = await input_channel.receive()
        assert received_error.message_type == MessageType.ERROR

        received_eos = await input_channel.receive()
        assert received_eos.message_type == MessageType.END_OF_STREAM

        await output_channel.close()
        await input_channel.close()
