"""Tests for Vectis ZMQ channels."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vectis import Message
from vectis.communication.enums import BackpressureMode, DistributionMode
from vectis.exceptions import BackpressureDroppedError, ChannelClosedError


def zmq_available() -> bool:
    """Check if pyzmq is installed."""
    try:
        import zmq  # noqa: F401

        return True
    except ImportError:
        return False


# Skip entire module if zmq is not available
pytestmark = pytest.mark.skipif(not zmq_available(), reason="pyzmq not installed")


# Only import if zmq is available
if zmq_available():
    from vectis.communication.channels.zmq import ZmqInputChannel, ZmqOutputChannel


# =============================================================================
# TestZmqOutputChannel
# =============================================================================


class TestZmqOutputChannel:
    """Tests for ZmqOutputChannel."""

    @pytest.mark.asyncio
    async def test_connect_creates_push_socket(self, json_serializer):
        """PUSH for COMPETING."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqOutputChannel(
            context=mock_context,
            endpoint="tcp://localhost:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
            name="test-output",
        )

        await channel.connect()

        import zmq

        mock_context.socket.assert_called_once_with(zmq.PUSH)
        mock_socket.connect.assert_called_once_with("tcp://localhost:5555")

        await channel.close()

    @pytest.mark.asyncio
    async def test_connect_creates_pub_socket_for_fanout(self, json_serializer):
        """PUB for FAN_OUT."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqOutputChannel(
            context=mock_context,
            endpoint="tcp://localhost:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.FAN_OUT,
            name="test-output",
        )

        await channel.connect()

        import zmq

        mock_context.socket.assert_called_once_with(zmq.PUB)

        await channel.close()

    @pytest.mark.asyncio
    async def test_connect_applies_hwm_setting(self, json_serializer):
        """SNDHWM set."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqOutputChannel(
            context=mock_context,
            endpoint="tcp://localhost:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
            high_water_mark=500,
        )

        await channel.connect()

        import zmq

        # Check that SNDHWM was set
        calls = mock_socket.setsockopt.call_args_list
        hwm_call = [c for c in calls if c[0][0] == zmq.SNDHWM]
        assert len(hwm_call) == 1
        assert hwm_call[0][0][1] == 500

        await channel.close()

    @pytest.mark.asyncio
    async def test_send_without_connect_raises(self, json_serializer):
        """RuntimeError when not connected."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)

        channel = ZmqOutputChannel(
            context=mock_context,
            endpoint="tcp://localhost:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
        )

        message = Message.data(payload={"value": 1}, source_component="test")

        with pytest.raises(RuntimeError, match="not connected"):
            await channel.send(message)

    @pytest.mark.asyncio
    async def test_send_on_closed_channel_raises(self, json_serializer):
        """ChannelClosedError raised."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqOutputChannel(
            context=mock_context,
            endpoint="tcp://localhost:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
        )

        await channel.connect()
        await channel.close()

        message = Message.data(payload={"value": 1}, source_component="test")
        with pytest.raises(ChannelClosedError):
            await channel.send(message)

    @pytest.mark.asyncio
    async def test_send_serializes_message(self, json_serializer):
        """Serializer called."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = AsyncMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqOutputChannel(
            context=mock_context,
            endpoint="tcp://localhost:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
        )

        await channel.connect()

        message = Message.data(payload={"value": 42}, source_component="test")
        await channel.send(message)

        # Verify send was called with serialized data
        mock_socket.send.assert_called_once()
        call_args = mock_socket.send.call_args
        sent_data = call_args[0][0]
        assert isinstance(sent_data, bytes)

        # Verify we can deserialize it back
        restored = json_serializer.deserialize(sent_data)
        assert restored.payload == {"value": 42}

        await channel.close()

    @pytest.mark.asyncio
    async def test_send_with_drop_mode_uses_noblock(self, json_serializer):
        """NOBLOCK flag used in DROP mode."""
        import zmq
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = AsyncMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqOutputChannel(
            context=mock_context,
            endpoint="tcp://localhost:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
            backpressure_mode=BackpressureMode.DROP,
        )

        await channel.connect()

        message = Message.data(payload={"value": 1}, source_component="test")
        await channel.send(message)

        # Verify send was called with NOBLOCK flag
        mock_socket.send.assert_called_once()
        call_kwargs = mock_socket.send.call_args[1]
        assert call_kwargs["flags"] & zmq.NOBLOCK

        await channel.close()

    @pytest.mark.asyncio
    async def test_send_with_drop_mode_raises_on_again(self, json_serializer):
        """BackpressureDroppedError when zmq.Again raised."""
        import zmq
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = AsyncMock()
        mock_socket.close = MagicMock()
        mock_socket.send.side_effect = zmq.Again()
        mock_context.socket.return_value = mock_socket

        channel = ZmqOutputChannel(
            context=mock_context,
            endpoint="tcp://localhost:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
            backpressure_mode=BackpressureMode.DROP,
        )

        await channel.connect()

        message = Message.data(payload={"value": 1}, source_component="test")
        with pytest.raises(BackpressureDroppedError):
            await channel.send(message)

        await channel.close()

    @pytest.mark.asyncio
    async def test_close_closes_socket(self, json_serializer):
        """Socket closed."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqOutputChannel(
            context=mock_context,
            endpoint="tcp://localhost:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
        )

        await channel.connect()
        await channel.close()

        mock_socket.close.assert_called_once_with(linger=0)
        assert channel.is_closed is True


# =============================================================================
# TestZmqInputChannel
# =============================================================================


class TestZmqInputChannel:
    """Tests for ZmqInputChannel."""

    @pytest.mark.asyncio
    async def test_connect_binds_pull_socket(self, json_serializer):
        """PULL for COMPETING."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqInputChannel(
            context=mock_context,
            endpoint="tcp://*:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
            name="test-input",
        )

        await channel.connect()

        import zmq

        mock_context.socket.assert_called_once_with(zmq.PULL)
        mock_socket.bind.assert_called_once_with("tcp://*:5555")

        await channel.close()

    @pytest.mark.asyncio
    async def test_connect_binds_sub_socket_for_fanout(self, json_serializer):
        """SUB for FAN_OUT."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqInputChannel(
            context=mock_context,
            endpoint="tcp://*:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.FAN_OUT,
        )

        await channel.connect()

        import zmq

        mock_context.socket.assert_called_once_with(zmq.SUB)

        await channel.close()

    @pytest.mark.asyncio
    async def test_connect_subscribes_to_topics(self, json_serializer):
        """SUBSCRIBE called for topics."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqInputChannel(
            context=mock_context,
            endpoint="tcp://*:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.FAN_OUT,
            topics=[b"topic1", b"topic2"],
        )

        await channel.connect()

        import zmq

        # Check SUBSCRIBE was called for each topic
        subscribe_calls = [
            c for c in mock_socket.setsockopt.call_args_list if c[0][0] == zmq.SUBSCRIBE
        ]
        assert len(subscribe_calls) == 2
        topics = {c[0][1] for c in subscribe_calls}
        assert topics == {b"topic1", b"topic2"}

        await channel.close()

    @pytest.mark.asyncio
    async def test_connect_subscribes_to_all_when_no_topics(self, json_serializer):
        """Subscribes to empty string (all) when no topics."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqInputChannel(
            context=mock_context,
            endpoint="tcp://*:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.FAN_OUT,
            # No topics specified
        )

        await channel.connect()

        import zmq

        # Check SUBSCRIBE was called with empty string
        subscribe_calls = [
            c for c in mock_socket.setsockopt.call_args_list if c[0][0] == zmq.SUBSCRIBE
        ]
        assert len(subscribe_calls) == 1
        assert subscribe_calls[0][0][1] == b""

        await channel.close()

    @pytest.mark.asyncio
    async def test_receive_without_connect_raises(self, json_serializer):
        """RuntimeError when not connected."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)

        channel = ZmqInputChannel(
            context=mock_context,
            endpoint="tcp://*:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
        )

        with pytest.raises(RuntimeError, match="not connected"):
            await channel.receive()

    @pytest.mark.asyncio
    async def test_receive_on_closed_channel_raises(self, json_serializer):
        """ChannelClosedError raised."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqInputChannel(
            context=mock_context,
            endpoint="tcp://*:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
        )

        await channel.connect()
        await channel.close()

        with pytest.raises(ChannelClosedError):
            await channel.receive()

    @pytest.mark.asyncio
    async def test_receive_deserializes_message(self, json_serializer):
        """Returns Message."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = AsyncMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        # Prepare serialized message
        msg = Message.data(payload={"value": 42}, source_component="test")
        serialized = json_serializer.serialize(msg)
        mock_socket.recv.return_value = serialized

        channel = ZmqInputChannel(
            context=mock_context,
            endpoint="tcp://*:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
        )

        await channel.connect()

        result = await channel.receive()
        assert isinstance(result, Message)
        assert result.payload == {"value": 42}
        assert result.source_component == "test"

        await channel.close()

    @pytest.mark.asyncio
    async def test_set_handler_stores_callback(self, json_serializer):
        """Handler stored."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)

        channel = ZmqInputChannel(
            context=mock_context,
            endpoint="tcp://*:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
        )

        async def my_handler(message: Message[Any]) -> None:
            pass

        assert channel.handler is None
        channel.set_handler(my_handler)
        assert channel.handler is my_handler

    @pytest.mark.asyncio
    async def test_connect_applies_rcvhwm(self, json_serializer):
        """RCVHWM set."""
        import zmq.asyncio

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        mock_context.socket.return_value = mock_socket

        channel = ZmqInputChannel(
            context=mock_context,
            endpoint="tcp://*:5555",
            serializer=json_serializer,
            distribution_mode=DistributionMode.COMPETING,
            high_water_mark=250,
        )

        await channel.connect()

        import zmq

        # Check that RCVHWM was set
        calls = mock_socket.setsockopt.call_args_list
        hwm_call = [c for c in calls if c[0][0] == zmq.RCVHWM]
        assert len(hwm_call) == 1
        assert hwm_call[0][0][1] == 250

        await channel.close()
