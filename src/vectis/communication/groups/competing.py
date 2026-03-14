"""Competing channel group implementation."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import TYPE_CHECKING, Any

from vectis.communication.enums import CompetingStrategy
from vectis.messages import MessageType

if TYPE_CHECKING:
    from vectis.communication.protocols import OutputChannel
    from vectis.messages import Message

logger = logging.getLogger(__name__)


class CompetingChannelGroup:
    """Channel group that distributes DATA to one channel, ERROR/EOS to all.

    Competing distribution sends DATA messages to only one consumer
    (selected by strategy), while ERROR and END_OF_STREAM messages
    are sent to ALL consumers. This implements load balancing.

    Strategies:
        - ROUND_ROBIN: Rotate through channels sequentially
        - RANDOM: Select a random channel for each message

    Attributes:
        _channels: List of output channels to distribute to.
        _strategy: How to select the next channel for DATA.
        _current_index: Current position for round-robin (0-indexed).
        _name: Optional name for debugging.

    Example:
        >>> group = CompetingChannelGroup(
        ...     channels=[ch1, ch2],
        ...     strategy=CompetingStrategy.ROUND_ROBIN,
        ... )
        >>> await group.send(data_msg)  # Goes to ch1
        >>> await group.send(data_msg)  # Goes to ch2
        >>> await group.send(error_msg)  # Goes to ch1 AND ch2
    """

    def __init__(
        self,
        channels: list[OutputChannel] | None = None,
        strategy: CompetingStrategy = CompetingStrategy.ROUND_ROBIN,
        name: str | None = None,
    ) -> None:
        """Initialize the competing channel group.

        Args:
            channels: Initial list of output channels.
            strategy: Distribution strategy for DATA messages.
            name: Optional name for debugging/logging.
        """
        self._channels: list[OutputChannel] = list(channels) if channels else []
        self._strategy = strategy
        self._current_index = 0
        self._name = name or f"competing-{id(self)}"

    def add_channel(self, channel: OutputChannel) -> None:
        """Add an output channel to this group.

        Args:
            channel: The output channel to add.
        """
        self._channels.append(channel)
        logger.debug(
            "CompetingChannelGroup '%s' added channel (total: %d)",
            self._name,
            len(self._channels),
        )

    async def send(self, message: Message[Any]) -> None:
        """Send a message according to competing distribution rules.

        DATA messages go to one channel (selected by strategy).
        ERROR and END_OF_STREAM messages go to ALL channels.

        Args:
            message: The message to send.
        """
        if not self._channels:
            logger.warning(
                "CompetingChannelGroup '%s' has no channels, message dropped",
                self._name,
            )
            return

        if message.message_type == MessageType.DATA:
            # DATA goes to one channel
            channel = self._select_channel()
            await channel.send(message)
            logger.debug(
                "CompetingChannelGroup '%s' sent DATA to channel %d/%d",
                self._name,
                self._channels.index(channel) + 1,
                len(self._channels),
            )
        else:
            # ERROR and END_OF_STREAM go to all channels
            await asyncio.gather(
                *(channel.send(message) for channel in self._channels)
            )
            logger.debug(
                "CompetingChannelGroup '%s' broadcast %s to %d channels",
                self._name,
                message.message_type.value,
                len(self._channels),
            )

    def _select_channel(self) -> OutputChannel:
        """Select the next channel based on strategy.

        Returns:
            The selected output channel.
        """
        if self._strategy == CompetingStrategy.ROUND_ROBIN:
            channel = self._channels[self._current_index]
            self._current_index = (self._current_index + 1) % len(self._channels)
            return channel
        elif self._strategy == CompetingStrategy.RANDOM:
            return random.choice(self._channels)
        else:
            # Fallback to first channel (shouldn't happen)
            return self._channels[0]

    async def close(self) -> None:
        """Close all channels in this group."""
        await asyncio.gather(*(channel.close() for channel in self._channels))
        logger.debug("CompetingChannelGroup '%s' closed all channels", self._name)

    @property
    def channel_count(self) -> int:
        """Get the number of channels in this group."""
        return len(self._channels)

    @property
    def strategy(self) -> CompetingStrategy:
        """Get the current distribution strategy."""
        return self._strategy

    @property
    def name(self) -> str:
        """Get the group name."""
        return self._name
