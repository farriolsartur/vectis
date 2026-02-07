"""FlowForge join buffer for correlation and memory management.

This module provides the JoinBuffer class that manages message correlation
for stream joins with bounded memory, TTL-based expiration, and eviction.
"""

from __future__ import annotations

import logging
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterator

if TYPE_CHECKING:
    from flowforge.components.joining.config import JoinConfig
    from flowforge.messages import Message

logger = logging.getLogger(__name__)


@dataclass
class KeyEntry:
    """Entry for a single correlation key in the buffer.

    Attributes:
        messages: Mapping of source name to list of messages for this key.
        created_at: Timestamp when this key was first seen.
        last_updated: Timestamp of last message added.
    """

    messages: dict[str, list[Message[Any]]] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class JoinBuffer:
    """Memory-safe buffer for join correlation with TTL and eviction.

    The JoinBuffer stores messages keyed by correlation values, allowing
    efficient lookup and completion checking for stream joins. It enforces
    memory bounds through:

    - Maximum pending keys (max_pending_keys)
    - Maximum messages per source per key (max_messages_per_key)
    - TTL-based expiration (key_ttl_seconds)
    - Configurable eviction policy

    The buffer uses an OrderedDict to maintain insertion order, enabling
    efficient oldest-first eviction.

    Example:
        >>> buffer = JoinBuffer(config)
        >>> buffer.add_message("order-123", "orders", order_message)
        >>> buffer.add_message("order-123", "customers", customer_message)
        >>> if buffer.is_complete("order-123"):
        ...     messages = buffer.pop_key("order-123")
        ...     process_joined(messages)
    """

    def __init__(self, config: JoinConfig) -> None:
        """Initialize the join buffer.

        Args:
            config: Join configuration defining sources and limits.
        """
        self._config = config
        self._buffers: OrderedDict[Any, KeyEntry] = OrderedDict()
        self._total_message_count = 0

    def add_message(
        self,
        key: Any,
        source: str,
        message: Message[Any],
    ) -> bool:
        """Add a message to the buffer for a correlation key.

        Args:
            key: The correlation key value.
            source: The source component name.
            message: The message to buffer.

        Returns:
            True if message was added, False if rejected due to limits.

        Raises:
            ValueError: If source is not in the configured sources list.
        """
        if source not in self._config.sources:
            raise ValueError(
                f"Source '{source}' not in configured sources: {self._config.sources}"
            )

        # Check if we need to create a new key entry
        if key not in self._buffers:
            # Check max_pending_keys limit
            if len(self._buffers) >= self._config.max_pending_keys:
                return False  # Caller should handle eviction

            self._buffers[key] = KeyEntry()
            logger.debug("Created new key entry: %s", key)

        entry = self._buffers[key]

        # Initialize source list if needed
        if source not in entry.messages:
            entry.messages[source] = []

        # Check max_messages_per_key limit for this source
        if len(entry.messages[source]) >= self._config.max_messages_per_key:
            logger.warning(
                "Max messages per key reached for key=%s, source=%s (limit=%d)",
                key,
                source,
                self._config.max_messages_per_key,
            )
            return False

        entry.messages[source].append(message)
        entry.last_updated = datetime.now(timezone.utc)
        self._total_message_count += 1

        # Move to end to maintain LRU order (most recently updated last)
        self._buffers.move_to_end(key)

        logger.debug(
            "Added message to key=%s, source=%s (total sources: %d/%d)",
            key,
            source,
            len(entry.messages),
            len(self._config.sources),
        )

        return True

    def get_key(self, key: Any) -> dict[str, list[Message[Any]]]:
        """Get messages for a correlation key without removing.

        Args:
            key: The correlation key value.

        Returns:
            Mapping of source name to message list, empty dict if key not found.
        """
        entry = self._buffers.get(key)
        if entry is None:
            return {}
        return dict(entry.messages)

    def pop_key(self, key: Any) -> dict[str, list[Message[Any]]]:
        """Remove and return messages for a correlation key.

        Args:
            key: The correlation key value.

        Returns:
            Mapping of source name to message list, empty dict if key not found.
        """
        entry = self._buffers.pop(key, None)
        if entry is None:
            return {}

        # Update total message count
        for messages in entry.messages.values():
            self._total_message_count -= len(messages)

        logger.debug("Popped key=%s with %d sources", key, len(entry.messages))
        return dict(entry.messages)

    def has_key(self, key: Any) -> bool:
        """Check if a correlation key exists in the buffer.

        Args:
            key: The correlation key value.

        Returns:
            True if key exists in buffer.
        """
        return key in self._buffers

    def is_complete(self, key: Any) -> bool:
        """Check if a correlation key has messages from all sources.

        This only checks for INNER join completion (all sources present).
        For LEFT_OUTER and FULL_OUTER modes, use mode-specific checks.

        Args:
            key: The correlation key value.

        Returns:
            True if all configured sources have messages for this key.
        """
        entry = self._buffers.get(key)
        if entry is None:
            return False

        sources_present = set(entry.messages.keys())
        sources_required = set(self._config.sources)
        return sources_present >= sources_required

    def is_complete_for_mode(self, key: Any) -> bool:
        """Check if a key is complete based on the configured join mode.

        Args:
            key: The correlation key value.

        Returns:
            True if the key should be emitted based on join mode.
        """
        from flowforge.components.joining.config import JoinMode

        entry = self._buffers.get(key)
        if entry is None:
            return False

        sources_present = set(entry.messages.keys())

        if self._config.mode == JoinMode.INNER:
            # All sources required
            return sources_present >= set(self._config.sources)

        elif self._config.mode == JoinMode.LEFT_OUTER:
            # Primary source required
            return self._config.primary_source in sources_present

        else:  # FULL_OUTER
            # Emit when ANY source has data
            return len(sources_present) > 0

    def has_primary_source(self, key: Any) -> bool:
        """Check if the primary source has data for a key.

        Args:
            key: The correlation key value.

        Returns:
            True if primary source has messages, False otherwise.
        """
        if self._config.primary_source is None:
            return False

        entry = self._buffers.get(key)
        if entry is None:
            return False

        return self._config.primary_source in entry.messages

    def get_expired_keys(self, now: datetime | None = None) -> list[Any]:
        """Get all keys that have exceeded their TTL.

        Args:
            now: Current timestamp. Defaults to UTC now.

        Returns:
            List of expired correlation keys.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        ttl_seconds = self._config.key_ttl_seconds
        expired: list[Any] = []

        for key, entry in self._buffers.items():
            age = (now - entry.created_at).total_seconds()
            if age >= ttl_seconds:
                expired.append(key)

        return expired

    def get_keys_in_window(self, now: datetime | None = None) -> list[Any]:
        """Get keys that have exceeded the window but not TTL.

        These are keys that should potentially be emitted as partial joins
        depending on the join mode.

        Args:
            now: Current timestamp. Defaults to UTC now.

        Returns:
            List of keys past window but not expired.
        """
        if now is None:
            now = datetime.now(timezone.utc)

        window_seconds = self._config.window_seconds
        ttl_seconds = self._config.key_ttl_seconds
        windowed: list[Any] = []

        for key, entry in self._buffers.items():
            age = (now - entry.created_at).total_seconds()
            if window_seconds <= age < ttl_seconds:
                windowed.append(key)

        return windowed

    def evict_oldest(self) -> tuple[Any, dict[str, list[Message[Any]]]] | None:
        """Evict the oldest key from the buffer.

        Returns:
            Tuple of (key, messages) if evicted, None if buffer is empty.
        """
        if not self._buffers:
            return None

        # Get oldest key (first in OrderedDict)
        key = next(iter(self._buffers))
        messages = self.pop_key(key)

        logger.info("Evicted oldest key: %s", key)
        return (key, messages)

    def keys(self) -> Iterator[Any]:
        """Iterate over all correlation keys.

        Returns:
            Iterator of correlation keys in insertion order.
        """
        return iter(self._buffers.keys())

    def clear(self) -> None:
        """Clear all entries from the buffer."""
        self._buffers.clear()
        self._total_message_count = 0

    @property
    def pending_key_count(self) -> int:
        """Number of pending correlation keys."""
        return len(self._buffers)

    @property
    def pending_message_count(self) -> int:
        """Total number of messages across all keys."""
        return self._total_message_count

    @property
    def is_at_capacity(self) -> bool:
        """Check if buffer is at max_pending_keys capacity."""
        return len(self._buffers) >= self._config.max_pending_keys
