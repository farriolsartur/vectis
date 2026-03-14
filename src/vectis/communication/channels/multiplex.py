"""Multiplex input channel for combining multiple sources."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from vectis.exceptions import ChannelClosedError
from vectis.messages import Message, MessageType

if TYPE_CHECKING:
    from vectis.communication.protocols import InputChannel

logger = logging.getLogger(__name__)


class MultiplexInputChannel:
    """Combines multiple input channels into a single stream.

    Used when multiple sources connect to the same target component.
    Messages from all sources are combined in FIFO order (first arrival wins).

    END_OF_STREAM is only forwarded after ALL sources have sent END_OF_STREAM,
    ensuring the receiver processes all data from all sources.

    Implements the InputChannel protocol so components can use it
    as a drop-in replacement for InProcessInputChannel.
    """

    def __init__(
        self,
        channels: list[InputChannel],
        name: str | None = None,
        queue_size: int = 0,
    ) -> None:
        """Initialize the multiplex channel.

        Args:
            channels: List of input channels to combine.
            name: Optional name for debugging/logging.
        """
        self._channels = channels
        self._combined_queue: asyncio.Queue[Message[Any]] = asyncio.Queue(
            maxsize=queue_size
        )
        self._forwarder_tasks: list[asyncio.Task[None]] = []
        self._closed = False
        self._handler: Callable[[Message[Any]], Awaitable[None]] | None = None
        self._name = name or f"multiplex-{id(self)}"
        self._active_sources = len(channels)
        self._sources_lock = asyncio.Lock()

    async def start(self) -> None:
        """Start forwarding from all source channels."""
        for channel in self._channels:
            task = asyncio.create_task(self._forward_from(channel))
            self._forwarder_tasks.append(task)
        logger.debug(
            "MultiplexInputChannel '%s' started with %d sources",
            self._name,
            len(self._channels),
        )

    async def _forward_from(self, channel: InputChannel) -> None:
        """Forward messages from one channel to combined queue.

        END_OF_STREAM messages are held until all sources have finished.
        If a source terminates abnormally (close/cancel), we still track
        completion to ensure the combined EOS is eventually sent.
        """
        source_ended_normally = False
        try:
            while not self._closed:
                message = await channel.receive()

                if message.message_type == MessageType.END_OF_STREAM:
                    source_ended_normally = True
                    await self._mark_source_complete(message.source_component)
                    break  # This forwarder task is done
                else:
                    # Forward data and error messages immediately
                    await self._combined_queue.put(message)
        except ChannelClosedError:
            pass  # Channel closed - handle in finally
        except asyncio.CancelledError:
            pass  # Task cancelled - handle in finally
        finally:
            # Ensure we track completion even on abnormal termination
            if not source_ended_normally:
                channel_name = getattr(channel, "name", f"input-{id(channel)}")
                await self._mark_source_complete(channel_name)

    async def _mark_source_complete(self, source_name: str) -> None:
        """Mark a source as complete and send combined EOS if all done."""
        async with self._sources_lock:
            self._active_sources -= 1
            remaining = self._active_sources
            logger.debug(
                "MultiplexChannel '%s': source '%s' ended, %d sources remaining",
                self._name,
                source_name,
                remaining,
            )
            if remaining == 0:
                # All sources done, send combined END_OF_STREAM
                combined_eos = Message.end_of_stream(
                    source_component=self._name
                )
                await self._combined_queue.put(combined_eos)

    async def receive(self) -> Message[Any]:
        """Receive next message from any source (FIFO order).

        Returns:
            The next message from any of the combined channels.

        Raises:
            ChannelClosedError: If the channel has been closed.
        """
        if self._closed:
            raise ChannelClosedError(self._name)

        message = await self._combined_queue.get()
        logger.debug(
            "MultiplexChannel '%s' received %s from '%s'",
            self._name,
            message.message_type.value,
            message.source_component,
        )
        return message

    def set_handler(
        self,
        handler: Callable[[Message[Any]], Awaitable[None]],
    ) -> None:
        """Set callback handler for incoming messages.

        Args:
            handler: Async function to call for each message.
        """
        self._handler = handler
        logger.debug("MultiplexChannel '%s' handler set", self._name)

    async def close(self) -> None:
        """Close this channel and all underlying channels."""
        if not self._closed:
            self._closed = True
            for task in self._forwarder_tasks:
                task.cancel()
            for channel in self._channels:
                await channel.close()
            self._handler = None
            logger.debug("MultiplexChannel '%s' closed", self._name)

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
