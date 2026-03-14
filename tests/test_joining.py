"""Unit tests for Vectis stream joining module."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from vectis import Message
from vectis.components.joining import (
    EOSAction,
    EvictionPolicy,
    JoinBuffer,
    JoinConfig,
    JoinMode,
)
from vectis.components.registry import (
    get_component_registry,
    get_component_type_registry,
)


@pytest.fixture(autouse=True)
def clear_registries():
    """Clear registries before each test."""
    get_component_registry().clear()
    yield
    get_component_registry().clear()


# =============================================================================
# JoinConfig Tests
# =============================================================================


class TestJoinConfig:
    """Tests for JoinConfig validation."""

    def test_minimal_config(self):
        """Test creating config with minimal required fields."""
        config = JoinConfig(
            correlation_key_path="order_id",
            sources=["source1", "source2"],
        )
        assert config.correlation_key_path == "order_id"
        assert config.sources == ["source1", "source2"]
        assert config.mode == JoinMode.INNER
        assert config.max_pending_keys == 10_000

    def test_full_config(self):
        """Test creating config with all fields."""
        config = JoinConfig(
            correlation_key_path="$.order.id",
            mode=JoinMode.LEFT_OUTER,
            sources=["orders", "customers"],
            primary_source="orders",
            window_seconds=60.0,
            max_pending_keys=50_000,
            max_messages_per_key=50,
            key_ttl_seconds=120.0,
            eviction_policy=EvictionPolicy.ERROR,
            eos_action=EOSAction.DROP_INCOMPLETE,
        )
        assert config.mode == JoinMode.LEFT_OUTER
        assert config.primary_source == "orders"
        assert config.max_pending_keys == 50_000

    def test_string_enum_parsing(self):
        """Test that string values are parsed into enums."""
        config = JoinConfig(
            correlation_key_path="id",
            sources=["a", "b"],
            mode="inner",
            eviction_policy="drop_oldest",
            eos_action="emit_partial",
        )
        assert config.mode == JoinMode.INNER
        assert config.eviction_policy == EvictionPolicy.DROP_OLDEST
        assert config.eos_action == EOSAction.EMIT_PARTIAL

    def test_left_outer_requires_primary_source(self):
        """Test that LEFT_OUTER mode requires primary_source."""
        with pytest.raises(ValidationError) as exc_info:
            JoinConfig(
                correlation_key_path="id",
                sources=["a", "b"],
                mode=JoinMode.LEFT_OUTER,
            )
        assert "primary_source is required" in str(exc_info.value)

    def test_primary_source_must_be_in_sources(self):
        """Test that primary_source must be in sources list."""
        with pytest.raises(ValidationError) as exc_info:
            JoinConfig(
                correlation_key_path="id",
                sources=["a", "b"],
                mode=JoinMode.LEFT_OUTER,
                primary_source="c",
            )
        assert "must be in sources list" in str(exc_info.value)

    def test_sources_must_be_unique(self):
        """Test that sources must be unique."""
        with pytest.raises(ValidationError) as exc_info:
            JoinConfig(
                correlation_key_path="id",
                sources=["a", "b", "a"],
            )
        assert "must be unique" in str(exc_info.value)

    def test_sources_must_not_be_empty(self):
        """Test that sources list cannot be empty."""
        with pytest.raises(ValidationError):
            JoinConfig(
                correlation_key_path="id",
                sources=[],
            )

    def test_correlation_key_path_must_not_be_empty(self):
        """Test that correlation_key_path cannot be empty."""
        with pytest.raises(ValidationError):
            JoinConfig(
                correlation_key_path="",
                sources=["a"],
            )

    def test_window_seconds_must_be_positive(self):
        """Test that window_seconds must be positive."""
        with pytest.raises(ValidationError):
            JoinConfig(
                correlation_key_path="id",
                sources=["a"],
                window_seconds=0,
            )

    def test_max_pending_keys_must_be_positive(self):
        """Test that max_pending_keys must be at least 1."""
        with pytest.raises(ValidationError):
            JoinConfig(
                correlation_key_path="id",
                sources=["a"],
                max_pending_keys=0,
            )


# =============================================================================
# JoinBuffer Tests
# =============================================================================


@pytest.fixture
def basic_config() -> JoinConfig:
    """Create a basic join config for testing."""
    return JoinConfig(
        correlation_key_path="order_id",
        sources=["orders", "customers"],
        max_pending_keys=100,
        max_messages_per_key=10,
        key_ttl_seconds=60.0,
    )


@pytest.fixture
def left_outer_config() -> JoinConfig:
    """Create a LEFT_OUTER join config for testing."""
    return JoinConfig(
        correlation_key_path="order_id",
        mode=JoinMode.LEFT_OUTER,
        sources=["orders", "customers"],
        primary_source="orders",
    )


def create_message(source: str, payload: dict[str, Any]) -> Message[Any]:
    """Helper to create test messages."""
    return Message.data(payload=payload, source_component=source)


class TestJoinBuffer:
    """Tests for JoinBuffer operations."""

    def test_add_message_creates_key(self, basic_config):
        """Test that adding a message creates a key entry."""
        buffer = JoinBuffer(basic_config)
        msg = create_message("orders", {"order_id": "123"})

        assert buffer.add_message("123", "orders", msg)
        assert buffer.has_key("123")
        assert buffer.pending_key_count == 1
        assert buffer.pending_message_count == 1

    def test_add_message_multiple_sources(self, basic_config):
        """Test adding messages from multiple sources."""
        buffer = JoinBuffer(basic_config)
        msg1 = create_message("orders", {"order_id": "123"})
        msg2 = create_message("customers", {"order_id": "123"})

        buffer.add_message("123", "orders", msg1)
        buffer.add_message("123", "customers", msg2)

        messages = buffer.get_key("123")
        assert len(messages) == 2
        assert "orders" in messages
        assert "customers" in messages

    def test_add_message_rejects_unknown_source(self, basic_config):
        """Test that adding from unknown source raises error."""
        buffer = JoinBuffer(basic_config)
        msg = create_message("unknown", {"order_id": "123"})

        with pytest.raises(ValueError) as exc_info:
            buffer.add_message("123", "unknown", msg)
        assert "not in configured sources" in str(exc_info.value)

    def test_add_message_rejects_at_capacity(self, basic_config):
        """Test that adding returns False when at capacity."""
        config = JoinConfig(
            correlation_key_path="id",
            sources=["a"],
            max_pending_keys=2,
        )
        buffer = JoinBuffer(config)

        msg1 = create_message("a", {"id": "1"})
        msg2 = create_message("a", {"id": "2"})
        msg3 = create_message("a", {"id": "3"})

        assert buffer.add_message("1", "a", msg1)
        assert buffer.add_message("2", "a", msg2)
        assert not buffer.add_message("3", "a", msg3)  # Should fail

    def test_add_message_respects_max_per_key(self, basic_config):
        """Test that max_messages_per_key is enforced."""
        config = JoinConfig(
            correlation_key_path="id",
            sources=["a"],
            max_messages_per_key=2,
        )
        buffer = JoinBuffer(config)

        msg1 = create_message("a", {"id": "1"})
        msg2 = create_message("a", {"id": "1"})
        msg3 = create_message("a", {"id": "1"})

        assert buffer.add_message("1", "a", msg1)
        assert buffer.add_message("1", "a", msg2)
        assert not buffer.add_message("1", "a", msg3)  # Should fail

    def test_pop_key_removes_entry(self, basic_config):
        """Test that pop_key removes and returns the entry."""
        buffer = JoinBuffer(basic_config)
        msg = create_message("orders", {"order_id": "123"})

        buffer.add_message("123", "orders", msg)
        messages = buffer.pop_key("123")

        assert len(messages) == 1
        assert not buffer.has_key("123")
        assert buffer.pending_key_count == 0

    def test_pop_nonexistent_key(self, basic_config):
        """Test popping a key that doesn't exist."""
        buffer = JoinBuffer(basic_config)
        messages = buffer.pop_key("nonexistent")
        assert messages == {}

    def test_is_complete_inner_mode(self, basic_config):
        """Test completion check for INNER mode."""
        buffer = JoinBuffer(basic_config)
        msg1 = create_message("orders", {"order_id": "123"})
        msg2 = create_message("customers", {"order_id": "123"})

        buffer.add_message("123", "orders", msg1)
        assert not buffer.is_complete("123")

        buffer.add_message("123", "customers", msg2)
        assert buffer.is_complete("123")

    def test_is_complete_for_mode_left_outer(self, left_outer_config):
        """Test completion check for LEFT_OUTER mode."""
        buffer = JoinBuffer(left_outer_config)
        msg = create_message("orders", {"order_id": "123"})

        buffer.add_message("123", "orders", msg)
        assert buffer.is_complete_for_mode("123")  # Primary source present

    def test_has_primary_source(self, left_outer_config):
        """Test checking for primary source."""
        buffer = JoinBuffer(left_outer_config)
        msg1 = create_message("customers", {"order_id": "123"})
        msg2 = create_message("orders", {"order_id": "123"})

        buffer.add_message("123", "customers", msg1)
        assert not buffer.has_primary_source("123")

        buffer.add_message("123", "orders", msg2)
        assert buffer.has_primary_source("123")

    def test_get_expired_keys(self, basic_config):
        """Test getting expired keys based on TTL."""
        config = JoinConfig(
            correlation_key_path="id",
            sources=["a"],
            key_ttl_seconds=1.0,
        )
        buffer = JoinBuffer(config)
        msg = create_message("a", {"id": "1"})

        buffer.add_message("1", "a", msg)

        # Not expired yet
        now = datetime.now(timezone.utc)
        assert buffer.get_expired_keys(now) == []

        # Expired after TTL
        future = now + timedelta(seconds=2)
        assert buffer.get_expired_keys(future) == ["1"]

    def test_evict_oldest(self, basic_config):
        """Test evicting the oldest key."""
        buffer = JoinBuffer(basic_config)
        msg1 = create_message("orders", {"order_id": "1"})
        msg2 = create_message("orders", {"order_id": "2"})

        buffer.add_message("1", "orders", msg1)
        buffer.add_message("2", "orders", msg2)

        result = buffer.evict_oldest()
        assert result is not None
        key, messages = result
        assert key == "1"  # First added
        assert buffer.pending_key_count == 1

    def test_evict_empty_buffer(self, basic_config):
        """Test evicting from empty buffer."""
        buffer = JoinBuffer(basic_config)
        result = buffer.evict_oldest()
        assert result is None

    def test_keys_iterator(self, basic_config):
        """Test iterating over keys."""
        buffer = JoinBuffer(basic_config)
        msg1 = create_message("orders", {"order_id": "1"})
        msg2 = create_message("orders", {"order_id": "2"})

        buffer.add_message("1", "orders", msg1)
        buffer.add_message("2", "orders", msg2)

        keys = list(buffer.keys())
        assert keys == ["1", "2"]

    def test_clear(self, basic_config):
        """Test clearing the buffer."""
        buffer = JoinBuffer(basic_config)
        msg = create_message("orders", {"order_id": "1"})

        buffer.add_message("1", "orders", msg)
        buffer.clear()

        assert buffer.pending_key_count == 0
        assert buffer.pending_message_count == 0

    def test_is_at_capacity(self, basic_config):
        """Test capacity check."""
        config = JoinConfig(
            correlation_key_path="id",
            sources=["a"],
            max_pending_keys=2,
        )
        buffer = JoinBuffer(config)
        msg1 = create_message("a", {"id": "1"})
        msg2 = create_message("a", {"id": "2"})

        assert not buffer.is_at_capacity

        buffer.add_message("1", "a", msg1)
        assert not buffer.is_at_capacity

        buffer.add_message("2", "a", msg2)
        assert buffer.is_at_capacity


# =============================================================================
# JoinerMixin Correlation Key Extraction Tests
# =============================================================================


class TestCorrelationKeyExtraction:
    """Tests for correlation key extraction methods."""

    def test_simple_key_extraction(self):
        """Test simple key extraction from dict payload."""
        from vectis.components.joining.joiner import _extract_key_simple

        payload = {"order_id": "123", "name": "test"}
        assert _extract_key_simple(payload, "order_id") == "123"

    def test_nested_key_extraction(self):
        """Test nested key extraction using dot notation."""
        from vectis.components.joining.joiner import _extract_key_simple

        payload = {"order": {"id": "123", "status": "pending"}}
        assert _extract_key_simple(payload, "order.id") == "123"

    def test_deeply_nested_key_extraction(self):
        """Test deeply nested key extraction."""
        from vectis.components.joining.joiner import _extract_key_simple

        payload = {"data": {"order": {"details": {"id": "123"}}}}
        assert _extract_key_simple(payload, "data.order.details.id") == "123"

    def test_key_not_found_returns_none(self):
        """Test that missing key returns None."""
        from vectis.components.joining.joiner import _extract_key_simple

        payload = {"order_id": "123"}
        assert _extract_key_simple(payload, "missing") is None

    def test_nested_key_not_found_returns_none(self):
        """Test that missing nested key returns None."""
        from vectis.components.joining.joiner import _extract_key_simple

        payload = {"order": {"id": "123"}}
        assert _extract_key_simple(payload, "order.missing") is None


class TestJSONPathExtraction:
    """Tests for JSONPath key extraction."""

    @pytest.fixture
    def skip_if_no_jsonpath(self):
        """Skip test if jsonpath-ng is not installed."""
        try:
            import jsonpath_ng  # noqa: F401
        except ImportError:
            pytest.skip("jsonpath-ng not installed")

    def test_jsonpath_extraction(self, skip_if_no_jsonpath):
        """Test JSONPath extraction."""
        from vectis.components.joining.joiner import _extract_key_jsonpath

        payload = {"order": {"id": "123"}}
        assert _extract_key_jsonpath(payload, "$.order.id") == "123"

    def test_jsonpath_array_extraction(self, skip_if_no_jsonpath):
        """Test JSONPath extraction from array."""
        from vectis.components.joining.joiner import _extract_key_jsonpath

        payload = {"orders": [{"id": "123"}, {"id": "456"}]}
        assert _extract_key_jsonpath(payload, "$.orders[0].id") == "123"

    def test_jsonpath_not_found_returns_none(self, skip_if_no_jsonpath):
        """Test that missing JSONPath returns None."""
        from vectis.components.joining.joiner import _extract_key_jsonpath

        payload = {"order": {"id": "123"}}
        assert _extract_key_jsonpath(payload, "$.missing.path") is None


# =============================================================================
# Joiner Component Registration Tests
# =============================================================================


class TestJoinerRegistration:
    """Tests for Joiner type registration."""

    def test_joiner_type_registered(self):
        """Test that joiner type is registered."""
        registry = get_component_type_registry()
        assert "joiner" in registry.types

    def test_joiner_decorator_exists(self):
        """Test that @joiner decorator is available."""
        from vectis.components import joiner

        assert callable(joiner)

    def test_joiner_in_exports(self):
        """Test that Joiner is exported from components."""
        from vectis.components import Joiner, JoinerMixin

        assert Joiner is not None
        assert JoinerMixin is not None


# =============================================================================
# Eviction Policy Tests
# =============================================================================


class TestEvictionPolicies:
    """Tests for different eviction policies."""

    def test_drop_oldest_eviction(self):
        """Test DROP_OLDEST eviction policy."""
        config = JoinConfig(
            correlation_key_path="id",
            sources=["a"],
            max_pending_keys=2,
            eviction_policy=EvictionPolicy.DROP_OLDEST,
        )
        buffer = JoinBuffer(config)

        # Fill buffer
        buffer.add_message("1", "a", create_message("a", {"id": "1"}))
        buffer.add_message("2", "a", create_message("a", {"id": "2"}))

        # Evict oldest
        result = buffer.evict_oldest()
        assert result is not None
        key, _ = result
        assert key == "1"

        # Now can add new key
        assert buffer.add_message("3", "a", create_message("a", {"id": "3"}))

    def test_buffer_at_capacity_detection(self):
        """Test that buffer correctly reports capacity status."""
        config = JoinConfig(
            correlation_key_path="id",
            sources=["a"],
            max_pending_keys=2,
        )
        buffer = JoinBuffer(config)

        buffer.add_message("1", "a", create_message("a", {"id": "1"}))
        assert not buffer.is_at_capacity

        buffer.add_message("2", "a", create_message("a", {"id": "2"}))
        assert buffer.is_at_capacity


# =============================================================================
# EOS Action Tests
# =============================================================================


class TestEOSActions:
    """Tests for end-of-stream action configurations."""

    def test_emit_partial_config(self):
        """Test EMIT_PARTIAL EOS action configuration."""
        config = JoinConfig(
            correlation_key_path="id",
            sources=["a", "b"],
            eos_action=EOSAction.EMIT_PARTIAL,
        )
        assert config.eos_action == EOSAction.EMIT_PARTIAL

    def test_drop_incomplete_config(self):
        """Test DROP_INCOMPLETE EOS action configuration."""
        config = JoinConfig(
            correlation_key_path="id",
            sources=["a", "b"],
            eos_action=EOSAction.DROP_INCOMPLETE,
        )
        assert config.eos_action == EOSAction.DROP_INCOMPLETE

    def test_error_config(self):
        """Test ERROR EOS action configuration."""
        config = JoinConfig(
            correlation_key_path="id",
            sources=["a", "b"],
            eos_action=EOSAction.ERROR,
        )
        assert config.eos_action == EOSAction.ERROR


# =============================================================================
# FULL_OUTER Join Mode Tests
# =============================================================================


class TestFullOuterJoin:
    """Tests for FULL_OUTER join mode."""

    @pytest.fixture
    def full_outer_config(self) -> JoinConfig:
        """Create a FULL_OUTER join config for testing."""
        return JoinConfig(
            correlation_key_path="order_id",
            mode=JoinMode.FULL_OUTER,
            sources=["orders", "customers"],
        )

    def test_full_outer_complete_with_any_source(self, full_outer_config):
        """Test FULL_OUTER is complete when ANY source has data."""
        buffer = JoinBuffer(full_outer_config)
        msg = create_message("orders", {"order_id": "123"})

        buffer.add_message("123", "orders", msg)
        assert buffer.is_complete_for_mode("123")  # Should be True with just one source

    def test_full_outer_complete_with_all_sources(self, full_outer_config):
        """Test FULL_OUTER is also complete when all sources present."""
        buffer = JoinBuffer(full_outer_config)
        msg1 = create_message("orders", {"order_id": "123"})
        msg2 = create_message("customers", {"order_id": "123"})

        buffer.add_message("123", "orders", msg1)
        buffer.add_message("123", "customers", msg2)
        assert buffer.is_complete_for_mode("123")

    def test_full_outer_not_complete_without_data(self, full_outer_config):
        """Test FULL_OUTER returns False for non-existent key."""
        buffer = JoinBuffer(full_outer_config)
        assert not buffer.is_complete_for_mode("nonexistent")


# =============================================================================
# Joiner EOS Handling Tests
# =============================================================================


class TestJoinerEOSHandling:
    """Tests for Joiner.on_received_ending EOS handling.

    Verifies correct behavior for both direct per-source EOS messages
    and combined EOS from MultiplexInputChannel.
    """

    @pytest.fixture
    def make_joiner(self):
        """Create a concrete Joiner subclass for testing."""
        from unittest.mock import AsyncMock, patch

        from vectis.components.joining.joiner import Joiner
        from pydantic import BaseModel

        class StubConfig(BaseModel):
            pass

        class StubJoiner(Joiner[StubConfig]):
            async def on_joined(self, key, messages):
                pass

        def _factory(sources, **join_kwargs):
            join_config = JoinConfig(
                correlation_key_path="id",
                sources=sources,
                **join_kwargs,
            )
            j = StubJoiner("test-joiner", StubConfig(), join_config)
            # Prevent timeout loop from running in tests
            j._timeout_task = None
            return j

        return _factory

    @pytest.mark.asyncio
    async def test_multiplex_eos_marks_all_sources_ended(self, make_joiner):
        """EOS with source_component not in sources marks all sources ended."""
        j = make_joiner(sources=["a", "b"])
        eos = Message.end_of_stream(source_component="enricher-multiplex")

        await j.on_received_ending(eos)

        assert j._all_sources_ended is True
        assert j._sources_ended == {"a", "b"}

    @pytest.mark.asyncio
    async def test_direct_eos_tracks_individual_source(self, make_joiner):
        """EOS from a configured source only marks that source."""
        j = make_joiner(sources=["a", "b"])
        eos_a = Message.end_of_stream(source_component="a")

        await j.on_received_ending(eos_a)

        assert j._all_sources_ended is False
        assert j._sources_ended == {"a"}

    @pytest.mark.asyncio
    async def test_direct_eos_all_sources_triggers_flush(self, make_joiner):
        """Sequential per-source EOS messages trigger all-sources-ended."""
        j = make_joiner(sources=["a", "b"])
        eos_a = Message.end_of_stream(source_component="a")
        eos_b = Message.end_of_stream(source_component="b")

        await j.on_received_ending(eos_a)
        assert j._all_sources_ended is False

        await j.on_received_ending(eos_b)
        assert j._all_sources_ended is True
        assert j._sources_ended == {"a", "b"}

    @pytest.mark.asyncio
    async def test_multiplex_eos_flushes_pending_joins(self, make_joiner):
        """Pending buffer items are flushed on multiplex EOS."""
        from unittest.mock import AsyncMock

        j = make_joiner(sources=["a", "b"], eos_action=EOSAction.EMIT_PARTIAL)
        j.on_partial_join = AsyncMock()

        # Add a pending message that won't complete its join
        msg = Message.data(payload={"id": "key1"}, source_component="a")
        j._join_buffer.add_message("key1", "a", msg)

        eos = Message.end_of_stream(source_component="some-multiplex")
        await j.on_received_ending(eos)

        # Pending key should have been flushed
        assert j._join_buffer.pending_key_count == 0
        j.on_partial_join.assert_called_once()
        call_args = j.on_partial_join.call_args
        assert call_args[0][0] == "key1"  # key
        assert call_args[0][2] == "eos"  # reason

    @pytest.mark.asyncio
    async def test_multiplex_eos_sends_downstream_eos(self, make_joiner):
        """send_end_of_stream() is called after flush on multiplex EOS."""
        from unittest.mock import AsyncMock, MagicMock

        j = make_joiner(sources=["a", "b"])
        j._output_channel_group = MagicMock()
        j.send_end_of_stream = AsyncMock()

        eos = Message.end_of_stream(source_component="enricher-multiplex")
        await j.on_received_ending(eos)

        j.send_end_of_stream.assert_called_once()
