"""Tests for Vectis protocol definitions."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

import pytest

from vectis import (
    ChannelGroup,
    ControlChannel,
    InputChannel,
    OutputChannel,
    RetryPolicy,
    Serializer,
    Triggerable,
)
from vectis.messages import Message


class TestOutputChannelProtocol:
    """Tests for the OutputChannel protocol."""

    def test_is_runtime_checkable(self) -> None:
        """OutputChannel should be runtime_checkable."""
        # Create a mock implementation
        class MockOutputChannel:
            async def send(self, message: Message[Any]) -> None:
                pass

            async def close(self) -> None:
                pass

        instance = MockOutputChannel()
        assert isinstance(instance, OutputChannel)

    def test_non_conforming_rejected(self) -> None:
        """Classes missing methods should not match OutputChannel."""
        class IncompleteChannel:
            async def send(self, message: Message[Any]) -> None:
                pass
            # Missing close()

        instance = IncompleteChannel()
        assert not isinstance(instance, OutputChannel)


class TestInputChannelProtocol:
    """Tests for the InputChannel protocol."""

    def test_is_runtime_checkable(self) -> None:
        """InputChannel should be runtime_checkable."""
        class MockInputChannel:
            async def receive(self) -> Message[Any]:
                return Message.data(payload=None, source_component="mock")

            def set_handler(
                self, handler: Callable[[Message[Any]], Awaitable[None]]
            ) -> None:
                pass

            async def close(self) -> None:
                pass

        instance = MockInputChannel()
        assert isinstance(instance, InputChannel)

    def test_non_conforming_rejected(self) -> None:
        """Classes missing methods should not match InputChannel."""
        class IncompleteChannel:
            async def receive(self) -> Message[Any]:
                return Message.data(payload=None, source_component="mock")
            # Missing set_handler and close

        instance = IncompleteChannel()
        assert not isinstance(instance, InputChannel)


class TestChannelGroupProtocol:
    """Tests for the ChannelGroup protocol."""

    def test_is_runtime_checkable(self) -> None:
        """ChannelGroup should be runtime_checkable."""
        class MockChannelGroup:
            async def send(self, message: Message[Any]) -> None:
                pass

            async def close(self) -> None:
                pass

        instance = MockChannelGroup()
        assert isinstance(instance, ChannelGroup)


class TestSerializerABC:
    """Tests for the Serializer abstract base class."""

    def test_cannot_instantiate_directly(self) -> None:
        """Serializer should not be instantiable directly."""
        with pytest.raises(TypeError):
            Serializer()  # type: ignore[abstract]

    def test_subclass_must_implement_methods(self) -> None:
        """Serializer subclasses must implement abstract methods."""
        class IncompleteSerializer(Serializer):
            def serialize(self, message: Message[Any]) -> bytes:
                return b""
            # Missing deserialize

        with pytest.raises(TypeError):
            IncompleteSerializer()  # type: ignore[abstract]

    def test_complete_subclass_works(self) -> None:
        """Properly implemented Serializer subclass should work."""
        class MockSerializer(Serializer):
            def serialize(self, message: Message[Any]) -> bytes:
                return b"serialized"

            def deserialize(self, data: bytes) -> Message[Any]:
                return Message.data(payload=None, source_component="mock")

        instance = MockSerializer()
        assert instance.serialize(Message.data(None, "test")) == b"serialized"


class TestRetryPolicyProtocol:
    """Tests for the RetryPolicy protocol."""

    def test_is_runtime_checkable(self) -> None:
        """RetryPolicy should be runtime_checkable."""
        class MockRetryPolicy:
            def should_retry(self, attempt: int) -> bool:
                return attempt < 3

            def get_delay(self, attempt: int) -> float:
                return 1.0 * attempt

        instance = MockRetryPolicy()
        assert isinstance(instance, RetryPolicy)

    def test_mock_implementation_works(self) -> None:
        """Mock RetryPolicy should function correctly."""
        class ExponentialBackoff:
            def __init__(self, max_attempts: int = 5, base_delay: float = 1.0):
                self.max_attempts = max_attempts
                self.base_delay = base_delay

            def should_retry(self, attempt: int) -> bool:
                return attempt < self.max_attempts

            def get_delay(self, attempt: int) -> float:
                return self.base_delay * (2 ** attempt)

        policy = ExponentialBackoff(max_attempts=3, base_delay=0.5)
        assert policy.should_retry(0) is True
        assert policy.should_retry(2) is True
        assert policy.should_retry(3) is False
        assert policy.get_delay(0) == 0.5
        assert policy.get_delay(1) == 1.0
        assert policy.get_delay(2) == 2.0


class TestControlChannelProtocol:
    """Tests for the ControlChannel protocol."""

    def test_is_runtime_checkable(self) -> None:
        """ControlChannel should be runtime_checkable."""
        class MockControlChannel:
            async def connect(self) -> None:
                pass

            async def broadcast_ready(self, component_names: list[str]) -> None:
                pass

            async def wait_for_dependencies(
                self,
                dependencies: list[str],
                timeout: float | None = None,
            ) -> bool:
                return True

            async def close(self) -> None:
                pass

        instance = MockControlChannel()
        assert isinstance(instance, ControlChannel)


class TestTriggerableProtocol:
    """Tests for the Triggerable protocol."""

    def test_is_runtime_checkable(self) -> None:
        """Triggerable should be runtime_checkable."""
        class MockTriggerable:
            _stop_requested: bool = False

            async def run(self) -> None:
                while not self._stop_requested:
                    await self._do_work()

            async def _do_work(self) -> None:
                pass

            def request_stop(self) -> None:
                self._stop_requested = True

        instance = MockTriggerable()
        assert isinstance(instance, Triggerable)

    def test_non_conforming_rejected(self) -> None:
        """Classes missing attributes should not match Triggerable."""
        class IncompleteTriggerable:
            async def run(self) -> None:
                pass

            def request_stop(self) -> None:
                pass
            # Missing _stop_requested attribute

        instance = IncompleteTriggerable()
        assert not isinstance(instance, Triggerable)

    def test_mock_implementation_stop_behavior(self) -> None:
        """Mock Triggerable should handle stop requests."""
        class MockDataProvider:
            _stop_requested: bool = False
            call_count: int = 0

            async def run(self) -> None:
                while not self._stop_requested:
                    self.call_count += 1
                    if self.call_count >= 5:
                        break

            def request_stop(self) -> None:
                self._stop_requested = True

        provider = MockDataProvider()
        assert provider._stop_requested is False
        provider.request_stop()
        assert provider._stop_requested is True
