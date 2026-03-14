"""Tests for Vectis control channels."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from vectis.communication.sync.control import MultiprocessControlChannel


def zmq_available() -> bool:
    """Check if pyzmq is installed."""
    try:
        import zmq  # noqa: F401

        return True
    except ImportError:
        return False


# =============================================================================
# TestMultiprocessControlChannel
# =============================================================================


class TestMultiprocessControlChannel:
    """Tests for MultiprocessControlChannel."""

    @pytest.mark.asyncio
    async def test_broadcast_ready_marks_components(self):
        """Components marked ready."""
        with patch(
            "vectis.communication.sync.control._ControlManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.connect = MagicMock()
            mock_manager.mark_ready = MagicMock()
            mock_manager.check_ready = MagicMock(return_value=True)
            MockManager.return_value = mock_manager

            channel = MultiprocessControlChannel(
                address=("localhost", 50000),
                authkey=b"test-key",
            )

            await channel.connect()
            await channel.broadcast_ready(["comp1", "comp2"])

            mock_manager.mark_ready.assert_called_once_with(["comp1", "comp2"])

            await channel.close()

    @pytest.mark.asyncio
    async def test_broadcast_ready_without_connect_raises(self):
        """RuntimeError raised when not connected."""
        channel = MultiprocessControlChannel(
            address=("localhost", 50000),
            authkey=b"test-key",
        )

        with pytest.raises(RuntimeError, match="not connected"):
            await channel.broadcast_ready(["comp1"])

    @pytest.mark.asyncio
    async def test_wait_for_dependencies_returns_true_when_ready(self):
        """Returns True when all dependencies ready."""
        with patch(
            "vectis.communication.sync.control._ControlManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.connect = MagicMock()
            mock_manager.check_ready = MagicMock(return_value=True)
            MockManager.return_value = mock_manager

            channel = MultiprocessControlChannel(
                address=("localhost", 50000),
                authkey=b"test-key",
            )

            await channel.connect()
            result = await channel.wait_for_dependencies(["comp1", "comp2"])

            assert result is True

            await channel.close()

    @pytest.mark.asyncio
    async def test_wait_for_dependencies_timeout(self):
        """Returns False on timeout."""
        with patch(
            "vectis.communication.sync.control._ControlManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.connect = MagicMock()
            # Always return not ready
            mock_manager.check_ready = MagicMock(return_value=False)
            MockManager.return_value = mock_manager

            channel = MultiprocessControlChannel(
                address=("localhost", 50000),
                authkey=b"test-key",
                poll_interval=0.01,  # Fast polling for test
            )

            await channel.connect()
            result = await channel.wait_for_dependencies(
                ["comp1"], timeout=0.05
            )

            assert result is False

            await channel.close()

    @pytest.mark.asyncio
    async def test_wait_for_dependencies_empty_list(self):
        """Returns True immediately for empty list."""
        with patch(
            "vectis.communication.sync.control._ControlManager"
        ) as MockManager:
            mock_manager = MagicMock()
            mock_manager.connect = MagicMock()
            mock_manager.check_ready = MagicMock(return_value=True)
            MockManager.return_value = mock_manager

            channel = MultiprocessControlChannel(
                address=("localhost", 50000),
                authkey=b"test-key",
            )

            await channel.connect()
            result = await channel.wait_for_dependencies([])

            # Should return True (check_ready with empty deps returns True)
            assert result is True

            await channel.close()

    @pytest.mark.asyncio
    async def test_wait_for_dependencies_without_connect_raises(self):
        """RuntimeError raised when not connected."""
        channel = MultiprocessControlChannel(
            address=("localhost", 50000),
            authkey=b"test-key",
        )

        with pytest.raises(RuntimeError, match="not connected"):
            await channel.wait_for_dependencies(["comp1"])


# =============================================================================
# TestZmqControlChannel
# =============================================================================


@pytest.mark.skipif(not zmq_available(), reason="pyzmq not installed")
class TestZmqControlChannel:
    """Tests for ZmqControlChannel."""

    @pytest.mark.asyncio
    async def test_connect_binds_pub_socket(self):
        """PUB bound."""
        import zmq.asyncio

        from vectis.communication.sync.control import ZmqControlChannel

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_pub_socket = MagicMock()
        mock_sub_socket = MagicMock()
        mock_pub_socket.close = MagicMock()
        mock_sub_socket.close = MagicMock()

        # Return different sockets for PUB and SUB
        sockets = [mock_pub_socket, mock_sub_socket]
        mock_context.socket.side_effect = lambda t: sockets.pop(0)

        channel = ZmqControlChannel(
            bind_endpoint="tcp://*:5556",
            connect_endpoints=["tcp://localhost:5557"],
            context=mock_context,
            name="test-control",
        )

        await channel.connect()

        import zmq

        # First socket should be PUB
        assert mock_context.socket.call_count == 2
        first_call = mock_context.socket.call_args_list[0]
        assert first_call[0][0] == zmq.PUB

        mock_pub_socket.bind.assert_called_once_with("tcp://*:5556")

        await channel.close()

    @pytest.mark.asyncio
    async def test_connect_subscribes_to_ready_prefix(self):
        """Subscribes to 'ready:' prefix."""
        import zmq.asyncio

        from vectis.communication.sync.control import ZmqControlChannel

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_pub_socket = MagicMock()
        mock_sub_socket = MagicMock()
        mock_pub_socket.close = MagicMock()
        mock_sub_socket.close = MagicMock()

        sockets = [mock_pub_socket, mock_sub_socket]
        mock_context.socket.side_effect = lambda t: sockets.pop(0)

        channel = ZmqControlChannel(
            bind_endpoint="tcp://*:5556",
            connect_endpoints=["tcp://localhost:5557"],
            context=mock_context,
        )

        await channel.connect()

        import zmq

        # Check SUB socket subscribed to "ready:" prefix
        subscribe_calls = [
            c
            for c in mock_sub_socket.setsockopt.call_args_list
            if c[0][0] == zmq.SUBSCRIBE
        ]
        assert len(subscribe_calls) == 1
        assert subscribe_calls[0][0][1] == b"ready:"

        await channel.close()

    @pytest.mark.asyncio
    async def test_broadcast_ready_publishes_messages(self):
        """Messages sent for each component."""
        import zmq.asyncio

        from vectis.communication.sync.control import ZmqControlChannel

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_pub_socket = AsyncMock()
        mock_sub_socket = MagicMock()
        mock_pub_socket.close = MagicMock()
        mock_sub_socket.close = MagicMock()

        sockets = [mock_pub_socket, mock_sub_socket]
        mock_context.socket.side_effect = lambda t: sockets.pop(0)

        channel = ZmqControlChannel(
            bind_endpoint="tcp://*:5556",
            connect_endpoints=[],
            context=mock_context,
        )

        await channel.connect()
        await channel.broadcast_ready(["comp1", "comp2", "comp3"])

        # Should have sent 3 messages
        assert mock_pub_socket.send.call_count == 3

        await channel.close()

    @pytest.mark.asyncio
    async def test_broadcast_ready_message_format(self):
        """Format is 'ready:<name>'."""
        import zmq.asyncio

        from vectis.communication.sync.control import ZmqControlChannel

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_pub_socket = AsyncMock()
        mock_sub_socket = MagicMock()
        mock_pub_socket.close = MagicMock()
        mock_sub_socket.close = MagicMock()

        sockets = [mock_pub_socket, mock_sub_socket]
        mock_context.socket.side_effect = lambda t: sockets.pop(0)

        channel = ZmqControlChannel(
            bind_endpoint="tcp://*:5556",
            connect_endpoints=[],
            context=mock_context,
        )

        await channel.connect()
        await channel.broadcast_ready(["my-component"])

        mock_pub_socket.send.assert_called_once_with(b"ready:my-component")

        await channel.close()

    @pytest.mark.asyncio
    async def test_broadcast_ready_without_connect_raises(self):
        """RuntimeError when not connected."""
        import zmq.asyncio

        from vectis.communication.sync.control import ZmqControlChannel

        mock_context = MagicMock(spec=zmq.asyncio.Context)

        channel = ZmqControlChannel(
            bind_endpoint="tcp://*:5556",
            connect_endpoints=[],
            context=mock_context,
        )

        with pytest.raises(RuntimeError, match="not connected"):
            await channel.broadcast_ready(["comp1"])

    @pytest.mark.asyncio
    async def test_wait_for_dependencies_timeout(self):
        """Returns False on timeout."""
        import zmq.asyncio

        from vectis.communication.sync.control import ZmqControlChannel

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_pub_socket = MagicMock()
        mock_sub_socket = AsyncMock()
        mock_pub_socket.close = MagicMock()
        mock_sub_socket.close = MagicMock()

        # Make recv raise TimeoutError
        mock_sub_socket.recv.side_effect = asyncio.TimeoutError()

        sockets = [mock_pub_socket, mock_sub_socket]
        mock_context.socket.side_effect = lambda t: sockets.pop(0)

        channel = ZmqControlChannel(
            bind_endpoint="tcp://*:5556",
            connect_endpoints=["tcp://localhost:5557"],
            context=mock_context,
        )

        await channel.connect()

        result = await channel.wait_for_dependencies(["comp1"], timeout=0.1)
        assert result is False

        await channel.close()

    @pytest.mark.asyncio
    async def test_wait_for_dependencies_empty_list(self):
        """Returns True immediately for empty list."""
        import zmq.asyncio

        from vectis.communication.sync.control import ZmqControlChannel

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_pub_socket = MagicMock()
        mock_sub_socket = MagicMock()
        mock_pub_socket.close = MagicMock()
        mock_sub_socket.close = MagicMock()

        sockets = [mock_pub_socket, mock_sub_socket]
        mock_context.socket.side_effect = lambda t: sockets.pop(0)

        channel = ZmqControlChannel(
            bind_endpoint="tcp://*:5556",
            connect_endpoints=[],
            context=mock_context,
        )

        await channel.connect()

        result = await channel.wait_for_dependencies([])
        assert result is True

        await channel.close()

    @pytest.mark.asyncio
    async def test_wait_for_dependencies_receives_ready_messages(self):
        """Returns True when all ready messages received."""
        import zmq.asyncio

        from vectis.communication.sync.control import ZmqControlChannel

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_pub_socket = MagicMock()
        mock_sub_socket = AsyncMock()
        mock_pub_socket.close = MagicMock()
        mock_sub_socket.close = MagicMock()

        # Return ready messages for both dependencies
        mock_sub_socket.recv.side_effect = [
            b"ready:comp1",
            b"ready:comp2",
        ]

        sockets = [mock_pub_socket, mock_sub_socket]
        mock_context.socket.side_effect = lambda t: sockets.pop(0)

        channel = ZmqControlChannel(
            bind_endpoint="tcp://*:5556",
            connect_endpoints=["tcp://localhost:5557"],
            context=mock_context,
        )

        await channel.connect()

        result = await channel.wait_for_dependencies(
            ["comp1", "comp2"], timeout=1.0
        )
        assert result is True

        await channel.close()

    @pytest.mark.asyncio
    async def test_close_closes_both_sockets(self):
        """Both sockets closed."""
        import zmq.asyncio

        from vectis.communication.sync.control import ZmqControlChannel

        mock_context = MagicMock(spec=zmq.asyncio.Context)
        mock_pub_socket = MagicMock()
        mock_sub_socket = MagicMock()

        sockets = [mock_pub_socket, mock_sub_socket]
        mock_context.socket.side_effect = lambda t: sockets.pop(0)

        channel = ZmqControlChannel(
            bind_endpoint="tcp://*:5556",
            connect_endpoints=["tcp://localhost:5557"],
            context=mock_context,
        )

        await channel.connect()
        await channel.close()

        mock_pub_socket.close.assert_called_once_with(linger=0)
        mock_sub_socket.close.assert_called_once_with(linger=0)
