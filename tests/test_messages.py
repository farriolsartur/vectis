"""Tests for Vectis message types."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from vectis import Message, MessageType


class TestMessageType:
    """Tests for the MessageType enum."""

    def test_data_value(self) -> None:
        """MessageType.DATA should have correct value."""
        assert MessageType.DATA.value == "data"

    def test_error_value(self) -> None:
        """MessageType.ERROR should have correct value."""
        assert MessageType.ERROR.value == "error"

    def test_end_of_stream_value(self) -> None:
        """MessageType.END_OF_STREAM should have correct value."""
        assert MessageType.END_OF_STREAM.value == "end_of_stream"

    def test_all_members_exist(self) -> None:
        """MessageType should have exactly 3 members."""
        members = list(MessageType)
        assert len(members) == 3
        assert MessageType.DATA in members
        assert MessageType.ERROR in members
        assert MessageType.END_OF_STREAM in members


class TestMessage:
    """Tests for the Message class."""

    def test_create_with_required_fields(self) -> None:
        """Message can be created with minimum required fields."""
        msg = Message(
            message_type=MessageType.DATA,
            payload="test",
            source_component="test_component",
        )
        assert msg.message_type == MessageType.DATA
        assert msg.payload == "test"
        assert msg.source_component == "test_component"

    def test_auto_generates_id(self) -> None:
        """Message should auto-generate a UUID id."""
        msg = Message(
            message_type=MessageType.DATA,
            payload=None,
            source_component="test",
        )
        assert isinstance(msg.id, UUID)

    def test_unique_ids(self) -> None:
        """Each message should have a unique id."""
        msg1 = Message(
            message_type=MessageType.DATA,
            payload=None,
            source_component="test",
        )
        msg2 = Message(
            message_type=MessageType.DATA,
            payload=None,
            source_component="test",
        )
        assert msg1.id != msg2.id

    def test_auto_generates_timestamp(self) -> None:
        """Message should auto-generate a timestamp."""
        before = datetime.now(timezone.utc)
        msg = Message(
            message_type=MessageType.DATA,
            payload=None,
            source_component="test",
        )
        after = datetime.now(timezone.utc)
        assert before <= msg.timestamp <= after

    def test_payload_type_optional(self) -> None:
        """payload_type should be optional and default to None."""
        msg = Message(
            message_type=MessageType.DATA,
            payload={"key": "value"},
            source_component="test",
        )
        assert msg.payload_type is None

    def test_payload_type_stores_path(self) -> None:
        """payload_type can store a type path string."""
        msg = Message(
            message_type=MessageType.DATA,
            payload={"key": "value"},
            source_component="test",
            payload_type="mymodule.MyModel",
        )
        assert msg.payload_type == "mymodule.MyModel"

    def test_is_frozen(self) -> None:
        """Message should be immutable (frozen)."""
        msg = Message(
            message_type=MessageType.DATA,
            payload="test",
            source_component="test",
        )
        with pytest.raises(Exception):  # ValidationError for frozen model
            msg.payload = "modified"  # type: ignore[misc]


class TestMessageFactoryMethods:
    """Tests for Message factory methods."""

    def test_data_creates_data_message(self) -> None:
        """Message.data() should create a DATA message."""
        msg = Message.data(
            payload={"value": 42},
            source_component="provider",
        )
        assert msg.message_type == MessageType.DATA
        assert msg.payload == {"value": 42}
        assert msg.source_component == "provider"

    def test_data_with_payload_type(self) -> None:
        """Message.data() should accept payload_type."""
        msg = Message.data(
            payload={"value": 42},
            source_component="provider",
            payload_type="mymodule.MyPayload",
        )
        assert msg.payload_type == "mymodule.MyPayload"

    def test_error_creates_error_message(self) -> None:
        """Message.error() should create an ERROR message."""
        msg = Message.error(
            error="Something failed",
            source_component="component",
        )
        assert msg.message_type == MessageType.ERROR
        assert msg.payload == "Something failed"
        assert msg.source_component == "component"

    def test_error_accepts_exception(self) -> None:
        """Message.error() should accept Exception objects."""
        exc = ValueError("Invalid value")
        msg = Message.error(
            error=exc,
            source_component="component",
        )
        assert "Invalid value" in msg.payload

    def test_end_of_stream_creates_eos_message(self) -> None:
        """Message.end_of_stream() should create an END_OF_STREAM message."""
        msg = Message.end_of_stream(source_component="provider")
        assert msg.message_type == MessageType.END_OF_STREAM
        assert msg.payload is None
        assert msg.source_component == "provider"


class TestMessageProperties:
    """Tests for Message property helpers."""

    def test_is_data_true_for_data(self) -> None:
        """is_data should return True for DATA messages."""
        msg = Message.data(payload=None, source_component="test")
        assert msg.is_data is True
        assert msg.is_error is False
        assert msg.is_end_of_stream is False

    def test_is_error_true_for_error(self) -> None:
        """is_error should return True for ERROR messages."""
        msg = Message.error(error="fail", source_component="test")
        assert msg.is_data is False
        assert msg.is_error is True
        assert msg.is_end_of_stream is False

    def test_is_end_of_stream_true_for_eos(self) -> None:
        """is_end_of_stream should return True for EOS messages."""
        msg = Message.end_of_stream(source_component="test")
        assert msg.is_data is False
        assert msg.is_error is False
        assert msg.is_end_of_stream is True


class TestMessageSerialization:
    """Tests for Message serialization capabilities."""

    def test_model_dump_returns_dict(self) -> None:
        """Message.model_dump() should return a dictionary."""
        msg = Message.data(
            payload={"key": "value"},
            source_component="test",
        )
        data = msg.model_dump()
        assert isinstance(data, dict)
        assert "id" in data
        assert "timestamp" in data
        assert "message_type" in data
        assert "payload" in data
        assert "source_component" in data

    def test_model_dump_json_serializable(self) -> None:
        """Message.model_dump(mode='json') should be JSON-serializable."""
        msg = Message.data(
            payload={"key": "value"},
            source_component="test",
        )
        data = msg.model_dump(mode="json")
        # UUID and datetime should be converted to strings
        assert isinstance(data["id"], str)
        assert isinstance(data["timestamp"], str)

    def test_roundtrip_via_model_validate(self) -> None:
        """Message should survive dump/validate roundtrip."""
        original = Message.data(
            payload={"key": "value"},
            source_component="test",
            payload_type="mymodule.MyModel",
        )
        data = original.model_dump()
        restored = Message.model_validate(data)

        assert restored.id == original.id
        assert restored.message_type == original.message_type
        assert restored.payload == original.payload
        assert restored.source_component == original.source_component
        assert restored.payload_type == original.payload_type


class TestMessageWithFixtures:
    """Tests using pytest fixtures."""

    def test_sample_data_message(self, sample_data_message: Message[dict[str, Any]]) -> None:
        """Test with sample data message fixture."""
        assert sample_data_message.is_data
        assert sample_data_message.payload["value"] == 42

    def test_sample_error_message(self, sample_error_message: Message[str]) -> None:
        """Test with sample error message fixture."""
        assert sample_error_message.is_error
        assert "wrong" in sample_error_message.payload.lower()

    def test_sample_eos_message(self, sample_eos_message: Message[None]) -> None:
        """Test with sample EOS message fixture."""
        assert sample_eos_message.is_end_of_stream
        assert sample_eos_message.payload is None
