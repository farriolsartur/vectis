"""ZeroMQ channel implementations for distributed communication."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from flowforge.communication.enums import BackpressureMode, DistributionMode
from flowforge.communication.protocols import RetryPolicy, Serializer
from flowforge.exceptions import (
    BackpressureDroppedError,
    ChannelClosedError,
    ConnectionRetryExhaustedError,
)

if TYPE_CHECKING:
    import zmq
    import zmq.asyncio

    from flowforge.messages import Message

logger = logging.getLogger(__name__)


class _ZmqBaseChannel:
    """Base ZMQ channel with retryable connect/bind."""

    def __init__(
        self,
        context: "zmq.asyncio.Context",
        endpoint: str,
        serializer: Serializer,
        *,
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
        high_water_mark: int = 1000,
    ) -> None:
        self._context = context
        self._endpoint = endpoint
        self._serializer = serializer
        self._retry_policy = retry_policy
        self._high_water_mark = high_water_mark
        self._socket: "zmq.asyncio.Socket | None" = None
        self._connected = False
        self._closed = False
        self._name = name or f"zmq-{id(self)}"

        try:
            # Lazy import to prevent crashes
            import zmq as zmq_module

            self._zmq = zmq_module
        except ImportError as e:
            raise ImportError(
                "pyzmq package is required for ZeroMQ channels. "
                "Install with: pip install flowforge[distributed]"
            ) from e

    async def _connect_with_retry(self, bind: bool, socket_type: int) -> None:
        """Connect or bind socket with optional retry policy."""
        if self._connected:
            return

        attempt = 0
        while True:
            try:
                self._socket = self._context.socket(socket_type)
                self._socket.setsockopt(self._zmq.LINGER, 0)
                self._apply_hwm(self._socket)
                if bind:
                    self._socket.bind(self._endpoint)
                else:
                    self._socket.connect(self._endpoint)
                self._connected = True
                logger.info(
                    "Channel '%s' %s %s",
                    self._name,
                    "bound to" if bind else "connected to",
                    self._endpoint,
                )
                return
            except Exception as exc:
                if not self._retry_policy or not self._retry_policy.should_retry(
                    attempt
                ):
                    raise ConnectionRetryExhaustedError(
                        self._name,
                        self._endpoint,
                        attempt + 1,
                    ) from exc
                delay = self._retry_policy.get_delay(attempt)
                attempt += 1
                if self._socket is not None:
                    try:
                        self._socket.close(linger=0)
                    except Exception:
                        pass
                    self._socket = None
                logger.warning(
                    "Channel '%s' connect failed, retrying in %.2fs",
                    self._name,
                    delay,
                )
                await asyncio.sleep(delay)

    def _apply_hwm(self, socket: "zmq.asyncio.Socket") -> None:
        """Apply HWM settings (override in subclasses)."""
        _ = socket

    async def close(self) -> None:
        """Close the underlying ZMQ socket."""
        if not self._closed:
            self._closed = True
            if self._socket is not None:
                try:
                    self._socket.close(linger=0)
                except Exception:
                    pass
                self._socket = None
            logger.debug("Channel '%s' closed", self._name)

    @property
    def is_closed(self) -> bool:
        """Check if this channel is closed."""
        return self._closed

    @property
    def name(self) -> str:
        """Get channel name."""
        return self._name


class ZmqOutputChannel(_ZmqBaseChannel):
    """Output channel for ZeroMQ communication."""

    def __init__(
        self,
        context: "zmq.asyncio.Context",
        endpoint: str,
        serializer: Serializer,
        distribution_mode: DistributionMode,
        *,
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
        backpressure_mode: BackpressureMode = BackpressureMode.BLOCK,
        high_water_mark: int = 1000,
    ) -> None:
        super().__init__(
            context,
            endpoint,
            serializer,
            name=name,
            retry_policy=retry_policy,
            high_water_mark=high_water_mark,
        )
        self._distribution_mode = distribution_mode
        self._backpressure_mode = backpressure_mode

    async def connect(self) -> None:
        """Connect to receiver endpoint (receiver binds first)."""
        socket_type = (
            self._zmq.PUB
            if self._distribution_mode == DistributionMode.FAN_OUT
            else self._zmq.PUSH
        )
        await self._connect_with_retry(bind=False, socket_type=socket_type)

    def _apply_hwm(self, socket: "zmq.asyncio.Socket") -> None:
        socket.setsockopt(self._zmq.SNDHWM, self._high_water_mark)

    async def send(self, message: "Message[Any]") -> None:
        """Serialize and send a message."""
        if self._closed:
            raise ChannelClosedError(self._name)
        if not self._connected or self._socket is None:
            raise RuntimeError(
                f"Channel '{self._name}' is not connected. Call connect() first."
            )

        data = self._serializer.serialize(message)
        flags = 0
        if self._backpressure_mode == BackpressureMode.DROP:
            flags |= self._zmq.NOBLOCK

        try:
            await self._socket.send(data, flags=flags)
        except self._zmq.Again as exc:
            raise BackpressureDroppedError(self._name) from exc

        logger.debug(
            "Channel '%s' sent %s message",
            self._name,
            message.message_type.value,
        )


class ZmqInputChannel(_ZmqBaseChannel):
    """Input channel for ZeroMQ communication."""

    def __init__(
        self,
        context: "zmq.asyncio.Context",
        endpoint: str,
        serializer: Serializer,
        distribution_mode: DistributionMode,
        *,
        name: str | None = None,
        retry_policy: RetryPolicy | None = None,
        topics: list[bytes] | None = None,
        high_water_mark: int = 1000,
    ) -> None:
        super().__init__(
            context,
            endpoint,
            serializer,
            name=name,
            retry_policy=retry_policy,
            high_water_mark=high_water_mark,
        )
        self._distribution_mode = distribution_mode
        self._topics = topics
        self._handler: Callable[[Message[Any]], Awaitable[None]] | None = None

    async def connect(self) -> None:
        """Bind to endpoint and start receiving."""
        socket_type = (
            self._zmq.SUB
            if self._distribution_mode == DistributionMode.FAN_OUT
            else self._zmq.PULL
        )
        await self._connect_with_retry(bind=True, socket_type=socket_type)
        if self._socket and self._distribution_mode == DistributionMode.FAN_OUT:
            topics = self._topics or [b""]
            for topic in topics:
                self._socket.setsockopt(self._zmq.SUBSCRIBE, topic)

    def _apply_hwm(self, socket: "zmq.asyncio.Socket") -> None:
        socket.setsockopt(self._zmq.RCVHWM, self._high_water_mark)

    async def receive(self) -> "Message[Any]":
        """Receive and deserialize a message."""
        if self._closed:
            raise ChannelClosedError(self._name)
        if not self._connected or self._socket is None:
            raise RuntimeError(
                f"Channel '{self._name}' is not connected. Call connect() first."
            )

        data = await self._socket.recv()
        message = self._serializer.deserialize(data)
        logger.debug(
            "Channel '%s' received %s message from '%s'",
            self._name,
            message.message_type.value,
            message.source_component,
        )
        return message

    def set_handler(
        self,
        handler: Callable[[Message[Any]], Awaitable[None]],
    ) -> None:
        """Set handler for incoming messages."""
        self._handler = handler
        logger.debug("Channel '%s' handler set", self._name)

    @property
    def handler(self) -> Callable[[Message[Any]], Awaitable[None]] | None:
        """Get current message handler."""
        return self._handler
