"""FlowForge component mixins for sending and receiving messages.

This module provides mixins that add send/receive capabilities to components.
Mixins are designed to be combined with the Component base class.
"""

from __future__ import annotations

import logging
import asyncio
from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from flowforge.messages import Message, MessageType
from flowforge.exceptions import ChannelClosedError

if TYPE_CHECKING:
    from flowforge.communication import ChannelGroup, InputChannel

logger = logging.getLogger(__name__)


class SenderMixin:
    """Mixin that provides message sending capabilities.

    This mixin adds methods for sending data, error, and end-of-stream
    messages through an output channel group. It should be combined
    with the Component base class.

    The mixin expects `self.name` to be available (from Component).

    Attributes:
        _output_channel_group: The channel group for sending messages.
                               Set by the Engine during wiring.

    Example:
        >>> class MyProvider(DataProvider[MyConfig]):
        ...     async def run(self):
        ...         for i in range(10):
        ...             await self.send_data({"value": i})
        ...         await self.send_end_of_stream()
    """

    _output_channel_group: ChannelGroup | None = None

    async def send_data(
        self, payload: Any, payload_type: str | None = None
    ) -> None:
        """Send a DATA message with the given payload.

        Args:
            payload: The data to send. Can be any JSON-serializable value.
            payload_type: Optional type path for Pydantic model payloads
                          (e.g., 'mymodule.MyModel').

        Raises:
            RuntimeError: If no output channel group is configured.
        """
        if self._output_channel_group is None:
            raise RuntimeError(
                f"Component '{self._get_component_name()}' has no output channel group. "
                "Ensure the component is properly wired before sending."
            )

        message = Message.data(
            payload=payload,
            source_component=self._get_component_name(),
            payload_type=payload_type,
        )
        await self._output_channel_group.send(message)

    async def send_error(self, error: str | Exception) -> None:
        """Send an ERROR message.

        Error messages are propagated to all consumers regardless of
        the channel group's distribution mode.

        Args:
            error: Error message string or exception.

        Raises:
            RuntimeError: If no output channel group is configured.
        """
        if self._output_channel_group is None:
            raise RuntimeError(
                f"Component '{self._get_component_name()}' has no output channel group. "
                "Ensure the component is properly wired before sending."
            )

        message = Message.error(
            error=error,
            source_component=self._get_component_name(),
        )
        await self._output_channel_group.send(message)

    async def send_end_of_stream(self) -> None:
        """Send an END_OF_STREAM message.

        This signals that no more messages will be sent from this component.
        END_OF_STREAM messages are propagated to all consumers regardless
        of the channel group's distribution mode.

        Raises:
            RuntimeError: If no output channel group is configured.
        """
        if self._output_channel_group is None:
            raise RuntimeError(
                f"Component '{self._get_component_name()}' has no output channel group. "
                "Ensure the component is properly wired before sending."
            )

        message = Message.end_of_stream(
            source_component=self._get_component_name(),
        )
        await self._output_channel_group.send(message)

    def _get_component_name(self) -> str:
        """Get the component name (expected from Component base class)."""
        # This will be available when mixed with Component
        return getattr(self, "name", "<unknown>")


class ReceiverMixin:
    """Mixin that provides message receiving capabilities.

    This mixin adds methods for receiving and dispatching messages from
    an input channel. It should be combined with the Component base class.

    The mixin provides a main receive loop (_listen_and_dispatch) that
    routes messages to the appropriate handler based on message type.

    Attributes:
        _input_channel: The input channel for receiving messages.
                        Set by the Engine during wiring.

    Example:
        >>> class MyAlgorithm(Algorithm[MyConfig]):
        ...     async def on_received_data(self, message):
        ...         print(f"Received: {message.payload}")
    """

    _input_channel: InputChannel | None = None
    _is_listening: bool = False

    async def _listen_and_dispatch(self) -> None:
        """Main receive loop that dispatches messages to handlers.

        This method runs continuously, receiving messages from the input
        channel and dispatching them to the appropriate handler based on
        message type:
            - DATA → on_received_data()
            - ERROR → on_received_error()
            - END_OF_STREAM → on_received_ending()

        The loop exits when an END_OF_STREAM message is received.

        Raises:
            RuntimeError: If no input channel is configured.
        """
        if self._input_channel is None:
            raise RuntimeError(
                f"Component '{self._get_component_name()}' has no input channel. "
                "Ensure the component is properly wired before listening."
            )

        self._is_listening = True
        try:
            while self._is_listening:
                try:
                    message = await self._input_channel.receive()
                except ChannelClosedError:
                    logger.debug(
                        "Component '%s' input channel closed, stopping listener",
                        self._get_component_name(),
                    )
                    break
                except asyncio.CancelledError:
                    break
                await self._dispatch_message(message)

                # Exit loop on END_OF_STREAM
                if message.is_end_of_stream:
                    break
        finally:
            self._is_listening = False

    async def _dispatch_message(self, message: Message[Any]) -> None:
        """Route a message to the appropriate handler.

        Args:
            message: The message to dispatch.
        """
        if message.message_type == MessageType.DATA:
            await self.on_received_data(message)
        elif message.message_type == MessageType.ERROR:
            await self.on_received_error(message)
        elif message.message_type == MessageType.END_OF_STREAM:
            await self.on_received_ending(message)

    @abstractmethod
    async def on_received_data(self, message: Message[Any]) -> None:
        """Handle a received DATA message.

        This method must be implemented by subclasses to define how
        data messages are processed.

        Args:
            message: The DATA message received.
        """
        ...

    async def on_received_error(self, message: Message[Any]) -> None:
        """Handle a received ERROR message.

        The default implementation logs the error. Override this method
        for custom error handling.

        Args:
            message: The ERROR message received.
        """
        logger.error(
            "Component '%s' received error from '%s': %s",
            self._get_component_name(),
            message.source_component,
            message.payload,
        )

    async def on_received_ending(self, message: Message[Any]) -> None:
        """Handle a received END_OF_STREAM message.

        The default implementation is a no-op. Override this method
        to perform cleanup or finalization when the stream ends.

        Args:
            message: The END_OF_STREAM message received.
        """
        logger.debug(
            "Component '%s' received END_OF_STREAM from '%s'",
            self._get_component_name(),
            message.source_component,
        )

    def _get_component_name(self) -> str:
        """Get the component name (expected from Component base class)."""
        return getattr(self, "name", "<unknown>")


class ProcessorMixin(SenderMixin, ReceiverMixin):
    """Mixin for components that both receive and send data.

    This mixin combines SenderMixin and ReceiverMixin for components
    that act as processors in the middle of a pipeline - receiving
    data from upstream, processing it, and sending results downstream.

    This is a convenience mixin; components can also inherit from
    both SenderMixin and ReceiverMixin directly.

    Example:
        >>> class TransformAlgorithm(Algorithm[MyConfig], ProcessorMixin):
        ...     async def on_received_data(self, message):
        ...         transformed = self.transform(message.payload)
        ...         await self.send_data(transformed)
    """

    pass
