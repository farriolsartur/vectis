"""Shared pytest fixtures for Vectis tests."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import pytest

from vectis import Message, MessageType
from vectis.communication.serialization.json_serializer import JSONSerializer
from vectis.communication.sync.retry import ExponentialBackoffPolicy


@pytest.fixture
def sample_data_message() -> Message[dict[str, Any]]:
    """Create a sample DATA message for testing."""
    return Message.data(
        payload={"value": 42, "name": "test"},
        source_component="test_provider",
    )


@pytest.fixture
def sample_error_message() -> Message[str]:
    """Create a sample ERROR message for testing."""
    return Message.error(
        error="Something went wrong",
        source_component="test_component",
    )


@pytest.fixture
def sample_eos_message() -> Message[None]:
    """Create a sample END_OF_STREAM message for testing."""
    return Message.end_of_stream(source_component="test_provider")


@pytest.fixture
def json_serializer() -> JSONSerializer:
    """JSON serializer for channel tests."""
    return JSONSerializer()


@pytest.fixture
def fast_retry_policy() -> ExponentialBackoffPolicy:
    """Fast retry policy for tests."""
    return ExponentialBackoffPolicy(
        base_delay=0.001, max_delay=0.01, max_attempts=3, jitter=0.0
    )
