"""Vectis communication protocols and abstract base classes.

This module defines the contracts for all communication-related components.
Concrete implementations are provided in submodules (channels/, serialization/, sync/).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol, runtime_checkable

if TYPE_CHECKING:
    from vectis.messages.message import Message


@runtime_checkable
class OutputChannel(Protocol):
    """Protocol for sending messages to consumers.

    OutputChannels are the sender's view of a communication channel.
    They handle serialization (if needed) and delivery to receivers.

    Implementations:
        - InProcessOutputChannel: Uses asyncio.Queue
        - MultiprocessOutputChannel: Uses multiprocessing.Queue
        - ZmqOutputChannel: Uses ZeroMQ sockets
    """

    async def send(self, message: Message[Any]) -> None:
        """Send a message through this channel.

        Args:
            message: The message to send.

        Raises:
            ChannelClosedError: If the channel has been closed.
        """
        ...

    async def close(self) -> None:
        """Close this channel and release resources.

        After closing, send() will raise ChannelClosedError.
        """
        ...


@runtime_checkable
class InputChannel(Protocol):
    """Protocol for receiving messages from producers.

    InputChannels are the receiver's view of a communication channel.
    They handle deserialization (if needed) and message delivery.

    Implementations:
        - InProcessInputChannel: Uses asyncio.Queue
        - MultiprocessInputChannel: Uses multiprocessing.Queue
        - ZmqInputChannel: Uses ZeroMQ sockets
    """

    async def receive(self) -> Message[Any]:
        """Receive the next message from this channel.

        Blocks until a message is available.

        Returns:
            The next message from the channel.

        Raises:
            ChannelClosedError: If the channel has been closed.
        """
        ...

    def set_handler(
        self, handler: Callable[[Message[Any]], Awaitable[None]]
    ) -> None:
        """Set a callback handler for incoming messages.

        When set, messages are automatically dispatched to this handler
        instead of being queued for receive().

        Args:
            handler: Async function to call for each message.
        """
        ...

    async def close(self) -> None:
        """Close this channel and release resources.

        After closing, receive() will raise ChannelClosedError.
        """
        ...


@runtime_checkable
class ChannelGroup(Protocol):
    """Protocol for managing multiple output channels as a unit.

    ChannelGroups handle distribution logic (fan-out vs competing)
    and provide a unified interface for sending to multiple consumers.

    Implementations:
        - FanOutChannelGroup: Sends to all channels
        - CompetingChannelGroup: Sends DATA to one, ERROR/EOS to all
    """

    async def send(self, message: Message[Any]) -> None:
        """Send a message through this channel group.

        Distribution behavior depends on the implementation:
        - FanOutChannelGroup: Sends to all channels
        - CompetingChannelGroup: DATA to one, ERROR/EOS to all

        Args:
            message: The message to send.
        """
        ...

    async def close(self) -> None:
        """Close all channels in this group."""
        ...


class Serializer(ABC):
    """Abstract base class for message serialization.

    Serializers convert Message objects to bytes for transport
    over multiprocess or distributed channels, and back again.

    Implementations:
        - JSONSerializer: Human-readable, slower
        - MessagePackSerializer: Binary, faster
    """

    @abstractmethod
    def serialize(self, message: Message[Any]) -> bytes:
        """Serialize a message to bytes.

        Args:
            message: The message to serialize.

        Returns:
            Serialized message as bytes.
        """
        ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Message[Any]:
        """Deserialize bytes back to a message.

        Args:
            data: Serialized message bytes.

        Returns:
            Reconstructed Message object.
        """
        ...


@runtime_checkable
class RetryPolicy(Protocol):
    """Protocol for connection retry behavior.

    RetryPolicy determines when and how long to wait between
    connection attempts during startup synchronization.

    Implementations:
        - ExponentialBackoffPolicy: Exponentially increasing delays
    """

    def should_retry(self, attempt: int) -> bool:
        """Determine if another retry attempt should be made.

        Args:
            attempt: The number of attempts made so far (0-indexed).

        Returns:
            True if another attempt should be made, False to give up.
        """
        ...

    def get_delay(self, attempt: int) -> float:
        """Get the delay before the next retry attempt.

        Args:
            attempt: The number of attempts made so far (0-indexed).

        Returns:
            Seconds to wait before the next attempt.
        """
        ...


@runtime_checkable
class ControlChannel(Protocol):
    """Protocol for coordinating component startup across workers.

    ControlChannels enable engines to communicate which components
    are ready, allowing dependent components to start in order.

    Implementations:
        - MultiprocessControlChannel: Same-host coordination
        - ZmqControlChannel: Cross-host coordination
    """

    async def connect(self) -> None:
        """Connect to the control channel.

        Must be called before other methods.
        """
        ...

    async def broadcast_ready(self, component_names: list[str]) -> None:
        """Announce that components are ready to receive.

        Args:
            component_names: Names of components that are now ready.
        """
        ...

    async def wait_for_dependencies(
        self,
        dependencies: list[str],
        timeout: float | None = None,
    ) -> bool:
        """Wait for required components to become ready.

        Args:
            dependencies: Component names to wait for.
            timeout: Maximum seconds to wait (None = forever).

        Returns:
            True if all dependencies are ready, False if timeout.
        """
        ...

    async def close(self) -> None:
        """Close the control channel and release resources."""
        ...
