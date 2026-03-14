"""Fan-out channel group implementation."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vectis.communication.protocols import OutputChannel
    from vectis.messages import Message

logger = logging.getLogger(__name__)


class FanOutChannelGroup:
    """Channel group that sends messages to ALL channels.

    Fan-out distribution sends every message (DATA, ERROR, END_OF_STREAM)
    to all connected output channels. This is useful when multiple
    consumers need to process the same data independently.

    Attributes:
        _channels: List of output channels to send to.
        _name: Optional name for debugging.

    Example:
        >>> group = FanOutChannelGroup(channels=[ch1, ch2, ch3])
        >>> await group.send(message)  # Sends to ch1, ch2, ch3
    """

    def __init__(
        self,
        channels: list[OutputChannel] | None = None,
        name: str | None = None,
    ) -> None:
        """Initialize the fan-out channel group.

        Args:
            channels: Initial list of output channels.
            name: Optional name for debugging/logging.
        """
        self._channels: list[OutputChannel] = list(channels) if channels else []
        self._name = name or f"fanout-{id(self)}"

    def add_channel(self, channel: OutputChannel) -> None:
        """Add an output channel to this group.

        Args:
            channel: The output channel to add.
        """
        self._channels.append(channel)
        logger.debug(
            "FanOutChannelGroup '%s' added channel (total: %d)",
            self._name,
            len(self._channels),
        )

    async def send(self, message: Message[Any]) -> None:
        """Send a message to ALL channels in this group.

        Messages are sent concurrently to all channels using
        asyncio.gather for efficiency.

        Args:
            message: The message to send.
        """
        if not self._channels:
            logger.warning(
                "FanOutChannelGroup '%s' has no channels, message dropped",
                self._name,
            )
            return

        # Send to all channels concurrently
        await asyncio.gather(*(channel.send(message) for channel in self._channels))

        logger.debug(
            "FanOutChannelGroup '%s' sent %s to %d channels",
            self._name,
            message.message_type.value,
            len(self._channels),
        )

    async def close(self) -> None:
        """Close all channels in this group."""
        await asyncio.gather(*(channel.close() for channel in self._channels))
        logger.debug("FanOutChannelGroup '%s' closed all channels", self._name)

    @property
    def channel_count(self) -> int:
        """Get the number of channels in this group."""
        return len(self._channels)

    @property
    def name(self) -> str:
        """Get the group name."""
        return self._name
