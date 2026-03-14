"""In-process channel implementations using asyncio.Queue."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from vectis.communication.enums import BackpressureMode
from vectis.exceptions import BackpressureDroppedError, ChannelClosedError

if TYPE_CHECKING:
    from vectis.messages import Message

logger = logging.getLogger(__name__)


class InProcessOutputChannel:
    """Output channel for in-process communication.

    Uses asyncio.Queue for message delivery. Multiple output channels
    can share the same queue (for competing consumers), or each can
    have its own queue (for fan-out).

    Attributes:
        _queue: The underlying asyncio.Queue.
        _closed: Whether the channel has been closed.
        _name: Optional name for debugging.

    Example:
        >>> import asyncio
        >>> queue = asyncio.Queue()
        >>> output = InProcessOutputChannel(queue, name="my-output")
        >>> # Use with corresponding InProcessInputChannel
    """

    def __init__(
        self,
        queue: asyncio.Queue[Message[Any]],
        name: str | None = None,
        backpressure_mode: BackpressureMode = BackpressureMode.BLOCK,
    ) -> None:
        """Initialize the output channel.

        Args:
            queue: The queue to send messages to.
            name: Optional name for debugging/logging.
        """
        self._queue = queue
        self._closed = False
        self._backpressure_mode = backpressure_mode
        self._name = name or f"output-{id(self)}"

    async def send(self, message: Message[Any]) -> None:
        """Send a message through this channel.

        Args:
            message: The message to send.

        Raises:
            ChannelClosedError: If the channel has been closed.
        """
        if self._closed:
            raise ChannelClosedError(self._name)

        if self._backpressure_mode == BackpressureMode.DROP:
            try:
                self._queue.put_nowait(message)
            except asyncio.QueueFull as exc:
                raise BackpressureDroppedError(self._name) from exc
        else:
            await self._queue.put(message)
        logger.debug(
            "Channel '%s' sent %s message",
            self._name,
            message.message_type.value,
        )

    async def close(self) -> None:
        """Close this channel.

        After closing, send() will raise ChannelClosedError.
        Note: This does not close the underlying queue as it may
        be shared with other channels.
        """
        if not self._closed:
            self._closed = True
            logger.debug("Channel '%s' closed", self._name)

    @property
    def is_closed(self) -> bool:
        """Check if this channel is closed."""
        return self._closed

    @property
    def name(self) -> str:
        """Get the channel name."""
        return self._name


class InProcessInputChannel:
    """Input channel for in-process communication.

    Receives messages from an asyncio.Queue. Supports two modes:
    1. Pull mode: Call receive() to get messages
    2. Push mode: Set a handler via set_handler() for callbacks

    Attributes:
        _queue: The underlying asyncio.Queue.
        _closed: Whether the channel has been closed.
        _handler: Optional callback for push mode.
        _name: Optional name for debugging.

    Example:
        >>> import asyncio
        >>> queue = asyncio.Queue()
        >>> input_ch = InProcessInputChannel(queue, name="my-input")
        >>> # Receive messages via await input_ch.receive()
    """

    def __init__(
        self,
        queue: asyncio.Queue[Message[Any]],
        name: str | None = None,
    ) -> None:
        """Initialize the input channel.

        Args:
            queue: The queue to receive messages from.
            name: Optional name for debugging/logging.
        """
        self._queue = queue
        self._closed = False
        self._handler: Callable[[Message[Any]], Awaitable[None]] | None = None
        self._name = name or f"input-{id(self)}"

    async def receive(self) -> Message[Any]:
        """Receive the next message from this channel.

        Blocks until a message is available.

        Returns:
            The next message from the channel.

        Raises:
            ChannelClosedError: If the channel has been closed.
        """
        if self._closed:
            raise ChannelClosedError(self._name)

        message = await self._queue.get()
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
        """Set a callback handler for incoming messages.

        When a handler is set, messages can be automatically dispatched
        to it instead of being retrieved via receive().

        Note: The actual dispatching is the responsibility of the caller
        (typically ReceiverMixin._listen_and_dispatch). This just stores
        the handler reference.

        Args:
            handler: Async function to call for each message.
        """
        self._handler = handler
        logger.debug("Channel '%s' handler set", self._name)

    async def close(self) -> None:
        """Close this channel.

        After closing, receive() will raise ChannelClosedError.
        """
        if not self._closed:
            self._closed = True
            self._handler = None
            logger.debug("Channel '%s' closed", self._name)

    @property
    def is_closed(self) -> bool:
        """Check if this channel is closed."""
        return self._closed

    @property
    def name(self) -> str:
        """Get the channel name."""
        return self._name

    @property
    def handler(self) -> Callable[[Message[Any]], Awaitable[None]] | None:
        """Get the current message handler."""
        return self._handler
