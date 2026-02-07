"""FlowForge JoinerMixin and Joiner base class for stream joins.

This module provides the JoinerMixin for adding join capabilities to
components, and the Joiner base class for components that join multiple
streams and forward results downstream.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic

from flowforge.components.base import Component, ConfigT
from flowforge.components.joining.buffer import JoinBuffer
from flowforge.components.joining.config import (
    EOSAction,
    EvictionPolicy,
    JoinConfig,
    JoinMode,
)
from flowforge.components.mixins import ReceiverMixin, SenderMixin
from flowforge.messages import Message

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _extract_key_simple(payload: Any, path: str) -> Any:
    """Extract key using simple dot notation.

    Args:
        payload: The message payload (dict or object).
        path: Dot-separated path like "order_id" or "order.id".

    Returns:
        The extracted key value, or None if not found.
    """
    parts = path.split(".")
    value = payload

    for part in parts:
        if isinstance(value, dict):
            value = value.get(part)
        else:
            value = getattr(value, part, None)

        if value is None:
            return None

    return value


def _extract_key_jsonpath(payload: Any, path: str) -> Any:
    """Extract key using JSONPath expression.

    Args:
        payload: The message payload.
        path: JSONPath expression like "$.order.id".

    Returns:
        The extracted key value, or None if not found.

    Raises:
        ImportError: If jsonpath-ng is not installed.
    """
    try:
        from jsonpath_ng import parse
    except ImportError as e:
        raise ImportError(
            "jsonpath-ng is required for JSONPath expressions. "
            "Install it with: pip install jsonpath-ng"
        ) from e

    expr = parse(path)
    matches = expr.find(payload)

    if matches:
        return matches[0].value
    return None


class JoinerMixin:
    """Mixin for adding join correlation capabilities to components.

    The JoinerMixin provides the infrastructure for correlating messages
    from multiple upstream sources based on a shared correlation key.
    It uses Message.source_component to identify which source sent each
    message.

    Components using this mixin must have:
    - _join_config: JoinConfig instance
    - _join_buffer: JoinBuffer instance
    - name attribute (from Component base class)

    The mixin provides:
    - Correlation key extraction
    - Message buffering and completion detection
    - Timeout handling for partial joins
    - Eviction handling for memory bounds
    - EOS handling for stream termination

    Subclasses must implement:
    - on_joined(): Called when a join is complete
    - on_partial_join(): Called for incomplete joins (optional)

    Example:
        >>> class MyJoiner(Joiner[MyConfig]):
        ...     async def on_joined(self, key, messages):
        ...         order = messages["orders"][0].payload
        ...         customer = messages["customers"][0].payload
        ...         await self.send_data({"order": order, "customer": customer})
    """

    _join_config: JoinConfig
    _join_buffer: JoinBuffer
    _timeout_task: asyncio.Task[None] | None = None
    _sources_ended: set[str]
    _all_sources_ended: bool = False

    def get_correlation_key(self, message: Message[Any]) -> Any:
        """Extract correlation key from a message payload.

        The key is extracted using the correlation_key_path from config.
        Supports simple dot notation and JSONPath expressions.

        Args:
            message: The message to extract key from.

        Returns:
            The correlation key value, or None if not found.
        """
        path = self._join_config.correlation_key_path
        payload = message.payload

        if path.startswith("$."):
            return _extract_key_jsonpath(payload, path)
        else:
            return _extract_key_simple(payload, path)

    @abstractmethod
    async def on_joined(
        self,
        key: Any,
        messages: dict[str, list[Message[Any]]],
    ) -> None:
        """Handle a completed join.

        Called when a correlation key has messages from all required
        sources (based on join mode). Implementations should process
        the joined data and send results downstream if needed.

        Args:
            key: The correlation key value.
            messages: Mapping of source name to list of messages.
        """
        ...

    async def on_partial_join(
        self,
        key: Any,
        messages: dict[str, list[Message[Any]]],
        reason: str,
    ) -> None:
        """Handle an incomplete join.

        Called when a join is emitted before completion, due to:
        - TTL expiration
        - Window timeout
        - Buffer eviction
        - End of stream

        The default implementation logs and drops the partial join.
        Override to implement custom partial join handling.

        Args:
            key: The correlation key value.
            messages: Mapping of source name to list of messages.
            reason: Why the partial join was emitted ("ttl", "eviction", "eos").
        """
        sources = list(messages.keys())
        expected = self._join_config.sources
        logger.warning(
            "Partial join for key=%s, reason=%s, sources=%s (expected=%s)",
            key,
            reason,
            sources,
            expected,
        )

    async def _process_for_join(self, message: Message[Any]) -> None:
        """Process an incoming message for join correlation.

        This method:
        1. Extracts the correlation key
        2. Handles buffer capacity (eviction if needed)
        3. Adds the message to the buffer
        4. Checks for join completion
        5. Emits joined result if complete

        Args:
            message: The incoming data message.
        """
        source = message.source_component
        key = self.get_correlation_key(message)

        if key is None:
            logger.warning(
                "Could not extract correlation key from message (source=%s, path=%s)",
                source,
                self._join_config.correlation_key_path,
            )
            return

        # Handle buffer capacity
        if self._join_buffer.is_at_capacity and not self._join_buffer.has_key(key):
            await self._handle_eviction()

        # Add message to buffer
        added = self._join_buffer.add_message(key, source, message)

        if not added:
            # Handle based on eviction policy
            policy = self._join_config.eviction_policy
            if policy == EvictionPolicy.ERROR:
                raise MemoryError(
                    f"Join buffer exceeded max_pending_keys ({self._join_config.max_pending_keys})"
                )
            elif policy == EvictionPolicy.DROP_NEWEST:
                logger.warning(
                    "Dropped message due to buffer capacity (key=%s, source=%s)",
                    key,
                    source,
                )
                return
            else:  # DROP_OLDEST
                await self._handle_eviction()
                # Retry add after eviction
                added = self._join_buffer.add_message(key, source, message)
                if not added:
                    logger.error("Failed to add message after eviction (key=%s)", key)
                    return

        # Check for completion
        if self._join_buffer.is_complete_for_mode(key):
            messages = self._join_buffer.pop_key(key)
            await self.on_joined(key, messages)

    async def _handle_eviction(self) -> None:
        """Handle buffer eviction when at capacity."""
        policy = self._join_config.eviction_policy

        if policy == EvictionPolicy.ERROR:
            raise MemoryError(
                f"Join buffer exceeded max_pending_keys ({self._join_config.max_pending_keys})"
            )

        elif policy == EvictionPolicy.DROP_OLDEST:
            result = self._join_buffer.evict_oldest()
            if result is not None:
                key, messages = result
                await self.on_partial_join(key, messages, "eviction")

        # DROP_NEWEST is handled in _process_for_join by rejecting new messages

    async def _check_timeouts(self) -> None:
        """Check for and handle expired and windowed keys."""
        # Handle TTL-expired keys
        expired = self._join_buffer.get_expired_keys()
        for key in expired:
            messages = self._join_buffer.pop_key(key)
            await self.on_partial_join(key, messages, "ttl")

        # Handle windowed keys for FULL_OUTER mode
        if self._join_config.mode == JoinMode.FULL_OUTER:
            windowed = self._join_buffer.get_keys_in_window()
            for key in windowed:
                messages = self._join_buffer.pop_key(key)
                await self.on_partial_join(key, messages, "window")

    async def _flush_all_pending(self, reason: str = "eos") -> None:
        """Flush all pending keys from the buffer.

        Called during EOS handling to process remaining keys
        based on eos_action configuration.

        Args:
            reason: The reason for flushing (for logging).
        """
        eos_action = self._join_config.eos_action

        if eos_action == EOSAction.ERROR:
            if self._join_buffer.pending_key_count > 0:
                raise RuntimeError(
                    f"End of stream with {self._join_buffer.pending_key_count} "
                    f"incomplete joins (eos_action=error)"
                )
            return

        # Process all remaining keys
        keys = list(self._join_buffer.keys())
        for key in keys:
            messages = self._join_buffer.pop_key(key)

            if eos_action == EOSAction.EMIT_PARTIAL:
                # Check if this would have been a complete join
                if self._is_join_satisfied(messages):
                    await self.on_joined(key, messages)
                else:
                    await self.on_partial_join(key, messages, reason)

            elif eos_action == EOSAction.DROP_INCOMPLETE:
                # Only emit if it would have been complete
                if self._is_join_satisfied(messages):
                    await self.on_joined(key, messages)
                else:
                    logger.debug(
                        "Dropped incomplete join (key=%s, sources=%s)",
                        key,
                        list(messages.keys()),
                    )

    def _is_join_satisfied(self, messages: dict[str, list[Message[Any]]]) -> bool:
        """Check if messages satisfy the join condition.

        Args:
            messages: The collected messages for a key.

        Returns:
            True if join condition is met.
        """
        sources_present = set(messages.keys())
        mode = self._join_config.mode

        if mode == JoinMode.INNER:
            return sources_present >= set(self._join_config.sources)
        elif mode == JoinMode.LEFT_OUTER:
            return self._join_config.primary_source in sources_present
        else:  # FULL_OUTER
            return len(sources_present) > 0

    async def _timeout_loop(self) -> None:
        """Background loop for checking timeouts.

        Runs periodically to check for expired keys and emit
        partial joins as needed.
        """
        check_interval = min(
            self._join_config.window_seconds / 2,
            self._join_config.key_ttl_seconds / 2,
            5.0,  # Check at least every 5 seconds
        )

        try:
            while True:
                await asyncio.sleep(check_interval)
                await self._check_timeouts()
        except asyncio.CancelledError:
            pass


class Joiner(Component[ConfigT], ReceiverMixin, SenderMixin, JoinerMixin, ABC, Generic[ConfigT]):
    """Base class for join-capable algorithms that can send downstream.

    Joiner combines Component, ReceiverMixin, SenderMixin, and JoinerMixin
    to create components that:
    1. Receive messages from multiple upstream sources
    2. Correlate them by a shared key
    3. Process joined results
    4. Send outputs downstream

    Subclasses must implement:
    - on_joined(): Process completed joins and send downstream

    Optionally override:
    - on_partial_join(): Handle incomplete joins
    - on_received_error(): Handle error messages
    - on_received_ending(): Customize EOS handling

    Example:
        >>> @joiner("order_enricher")
        ... class OrderEnricher(Joiner[EnricherConfig]):
        ...     async def on_joined(self, key, messages):
        ...         order = messages["orders"][0].payload
        ...         customer = messages["customers"][0].payload
        ...         enriched = {**order, "customer": customer}
        ...         await self.send_data(enriched)

    The joiner must be configured with a JoinConfig in the pipeline YAML:

        joiners:
          - name: order_enricher
            type: order_enricher
            join:
              correlation_key_path: "$.order_id"
              sources: [orders, customers]
              mode: inner
    """

    def __init__(
        self,
        name: str,
        config: ConfigT,
        join_config: JoinConfig,
    ) -> None:
        """Initialize the Joiner.

        Args:
            name: Unique identifier for this component instance.
            config: The validated component configuration.
            join_config: The join configuration.
        """
        super().__init__(name, config)
        self._join_config = join_config
        self._join_buffer = JoinBuffer(join_config)
        self._timeout_task = None
        self._sources_ended: set[str] = set()
        self._all_sources_ended = False

    async def on_start(self) -> None:
        """Start the timeout checking loop.

        Called by the Engine after wiring but before message processing.
        """
        self._timeout_task = asyncio.create_task(
            self._timeout_loop(),
            name=f"joiner-timeout-{self.name}",
        )
        logger.debug("Started timeout loop for joiner '%s'", self.name)

    async def on_stop(self) -> None:
        """Stop the timeout loop and flush pending joins.

        Called by the Engine during shutdown.
        """
        # Cancel timeout task
        if self._timeout_task is not None:
            self._timeout_task.cancel()
            try:
                await self._timeout_task
            except asyncio.CancelledError:
                pass
            self._timeout_task = None

        # Flush any remaining pending joins
        if self._join_buffer.pending_key_count > 0:
            logger.info(
                "Flushing %d pending joins on stop for '%s'",
                self._join_buffer.pending_key_count,
                self.name,
            )
            await self._flush_all_pending("stop")

        logger.debug("Joiner '%s' stopped", self.name)

    async def on_received_data(self, message: Message[Any]) -> None:
        """Process incoming data messages for join correlation.

        Routes messages to the join buffer based on their source_component.

        Args:
            message: The incoming DATA message.
        """
        await self._process_for_join(message)

    async def on_received_ending(self, message: Message[Any]) -> None:
        """Handle end-of-stream messages from sources.

        Tracks which sources have ended. When all sources have ended,
        flushes pending joins according to eos_action.

        Handles two scenarios:
        - Direct connection: EOS arrives per-source with source_component
          matching one of the configured join sources.
        - MultiplexInputChannel: A single combined EOS arrives with
          source_component set to the multiplex channel name (not in
          configured sources). The multiplex only sends this after ALL
          individual sources have sent EOS, so receiving it means every
          upstream source is done.

        Args:
            message: The END_OF_STREAM message.
        """
        source = message.source_component
        expected_sources = set(self._join_config.sources)

        if source not in expected_sources:
            # Combined EOS from MultiplexInputChannel. The multiplex only
            # emits this after ALL individual sources have sent EOS,
            # so receiving it means every upstream source is done.
            logger.debug(
                "Received combined EOS from '%s' for joiner '%s', "
                "treating as all sources ended",
                source,
                self.name,
            )
            self._sources_ended = set(expected_sources)
        else:
            # Per-source EOS (direct connection, no multiplex)
            self._sources_ended.add(source)
            logger.debug(
                "Source '%s' ended for joiner '%s' (%d/%d sources ended)",
                source,
                self.name,
                len(self._sources_ended),
                len(self._join_config.sources),
            )

        # Check if all sources have ended
        if self._sources_ended >= expected_sources:
            self._all_sources_ended = True
            logger.info(
                "All sources ended for joiner '%s', flushing %d pending keys",
                self.name,
                self._join_buffer.pending_key_count,
            )

            # Cancel timeout task since we're flushing everything
            if self._timeout_task is not None:
                self._timeout_task.cancel()
                try:
                    await self._timeout_task
                except asyncio.CancelledError:
                    pass
                self._timeout_task = None

            # Flush all pending joins
            await self._flush_all_pending("eos")

            # Send our own EOS downstream
            if self._output_channel_group is not None:
                await self.send_end_of_stream()

    @abstractmethod
    async def on_joined(
        self,
        key: Any,
        messages: dict[str, list[Message[Any]]],
    ) -> None:
        """Handle a completed join.

        Implementations should process the joined data and typically
        send the result downstream using send_data().

        Args:
            key: The correlation key value.
            messages: Mapping of source name to list of messages.
        """
        ...
