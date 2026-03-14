"""Tests for Vectis Phase 2: Component System.

This module tests:
- Component base class and config extraction
- SenderMixin and ReceiverMixin
- DataProvider and Algorithm classes
- ComponentTypeRegistry and ComponentRegistry
- Convenience decorators (@algorithm, @data_provider)
- ComponentFactory
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from vectis import (
    Algorithm,
    Component,
    ComponentFactory,
    ComponentNotFoundError,
    ComponentRegistry,
    ComponentTypeRegistry,
    DataProvider,
    EmptyConfig,
    Message,
    MessageType,
    PipelineConfigError,
    ProcessorMixin,
    ReceiverMixin,
    SenderMixin,
    Triggerable,
    algorithm,
    data_provider,
    get_component_registry,
    get_component_type_registry,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def mock_channel_group() -> AsyncMock:
    """Create a mock ChannelGroup for testing."""
    mock = AsyncMock()
    mock.send = AsyncMock()
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def mock_input_channel() -> AsyncMock:
    """Create a mock InputChannel for testing."""
    mock = AsyncMock()
    mock.receive = AsyncMock()
    mock.set_handler = MagicMock()
    mock.close = AsyncMock()
    return mock


@pytest.fixture(autouse=True)
def clear_registries():
    """Clear registries before each test to ensure isolation."""
    # Clear before test
    get_component_registry().clear()
    # Note: Don't clear type registry as it holds built-in types
    yield
    # Clear after test
    get_component_registry().clear()


# =============================================================================
# Test Config Classes
# =============================================================================


class SimpleConfig(BaseModel):
    """Simple test configuration."""

    value: int = 10
    name: str = "default"


class ThresholdConfig(BaseModel):
    """Configuration with threshold."""

    threshold: float = 0.5
    enabled: bool = True


# =============================================================================
# TestComponentTypeRegistry
# =============================================================================


class TestComponentTypeRegistry:
    """Tests for ComponentTypeRegistry."""

    def test_singleton_pattern(self):
        """Test that ComponentTypeRegistry is a singleton."""
        reg1 = ComponentTypeRegistry()
        reg2 = ComponentTypeRegistry()
        assert reg1 is reg2

    def test_builtin_types_registered(self):
        """Test that built-in types are registered on import."""
        registry = get_component_type_registry()
        assert "algorithm" in registry.types
        assert "data_provider" in registry.types

    def test_register_type(self):
        """Test registering a new component type."""
        registry = get_component_type_registry()

        # Create a mock base class
        class CustomComponent(Component[EmptyConfig]):
            pass

        # Register if not already registered
        if "custom" not in registry.types:
            registry.register_type("custom", CustomComponent)
            assert "custom" in registry.types
            assert registry.get_type("custom") is CustomComponent

    def test_register_duplicate_type_raises(self):
        """Test that registering duplicate type raises ValueError."""
        registry = get_component_type_registry()
        with pytest.raises(ValueError, match="already registered"):
            registry.register_type("algorithm", Algorithm)

    def test_get_nonexistent_type_raises(self):
        """Test that getting nonexistent type raises KeyError."""
        registry = get_component_type_registry()
        with pytest.raises(KeyError, match="not registered"):
            registry.get_type("nonexistent")

    def test_create_decorator(self):
        """Test creating a decorator for a component type."""
        registry = get_component_type_registry()
        decorator = registry.create_decorator("algorithm")
        assert callable(decorator)

    def test_create_decorator_nonexistent_type_raises(self):
        """Test creating decorator for nonexistent type raises KeyError."""
        registry = get_component_type_registry()
        with pytest.raises(KeyError, match="not registered"):
            registry.create_decorator("nonexistent")

    def test_types_property_returns_copy(self):
        """Test that types property returns a copy."""
        registry = get_component_type_registry()
        types = registry.types
        # Modifying returned dict shouldn't affect registry
        types["fake"] = None
        assert "fake" not in registry.types


# =============================================================================
# TestComponentRegistry
# =============================================================================


class TestComponentRegistry:
    """Tests for ComponentRegistry."""

    def test_singleton_pattern(self):
        """Test that ComponentRegistry is a singleton."""
        reg1 = ComponentRegistry()
        reg2 = ComponentRegistry()
        assert reg1 is reg2

    def test_register_and_get_component(self):
        """Test registering and retrieving a component."""
        registry = get_component_registry()

        class TestAlgo(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        registry.register_component("test_algo", TestAlgo, "algorithm")
        assert registry.get_component("test_algo") is TestAlgo

    def test_register_duplicate_component_raises(self):
        """Test that registering duplicate component raises ValueError."""
        registry = get_component_registry()

        class TestAlgo1(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        class TestAlgo2(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        registry.register_component("dupe_algo", TestAlgo1, "algorithm")
        with pytest.raises(ValueError, match="already registered"):
            registry.register_component("dupe_algo", TestAlgo2, "algorithm")

    def test_get_nonexistent_component_raises(self):
        """Test that getting nonexistent component raises ComponentNotFoundError."""
        registry = get_component_registry()
        with pytest.raises(ComponentNotFoundError) as exc_info:
            registry.get_component("nonexistent")
        assert exc_info.value.component_name == "nonexistent"

    def test_get_component_type(self):
        """Test getting the type of a registered component."""
        registry = get_component_registry()

        class TestAlgo(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        registry.register_component("typed_algo", TestAlgo, "algorithm")
        assert registry.get_component_type("typed_algo") == "algorithm"

    def test_components_property(self):
        """Test that components property returns mapping."""
        registry = get_component_registry()

        class TestAlgo(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        registry.register_component("prop_algo", TestAlgo, "algorithm")
        components = registry.components
        assert "prop_algo" in components
        assert components["prop_algo"] is TestAlgo


# =============================================================================
# TestComponent
# =============================================================================


class TestComponent:
    """Tests for Component base class."""

    def test_component_initialization(self):
        """Test component initialization with name and config."""

        class TestComp(Component[SimpleConfig]):
            pass

        config = SimpleConfig(value=42, name="test")
        comp = TestComp(name="my_component", config=config)
        assert comp.name == "my_component"
        assert comp.config.value == 42
        assert comp.config.name == "test"

    def test_get_config_class_with_explicit_type(self):
        """Test extracting config class from generic parameter."""

        class TestComp(Component[SimpleConfig]):
            pass

        config_class = TestComp.get_config_class()
        assert config_class is SimpleConfig

    def test_get_config_class_without_type_returns_empty(self):
        """Test that unparameterized component returns EmptyConfig."""
        # Note: This tests the fallback when no config type is specified
        config_class = Component.get_config_class()
        assert config_class is EmptyConfig

    def test_get_config_class_through_inheritance(self):
        """Test config class extraction through inheritance chain."""

        class MiddleClass(Component[ThresholdConfig]):
            pass

        class FinalClass(MiddleClass):
            pass

        config_class = FinalClass.get_config_class()
        assert config_class is ThresholdConfig

    @pytest.mark.asyncio
    async def test_lifecycle_hooks_default_noop(self):
        """Test that lifecycle hooks are no-ops by default."""

        class TestComp(Component[EmptyConfig]):
            pass

        comp = TestComp(name="test", config=EmptyConfig())
        # These should not raise
        await comp.on_start()
        await comp.on_stop()

    @pytest.mark.asyncio
    async def test_lifecycle_hooks_can_be_overridden(self):
        """Test that lifecycle hooks can be overridden."""
        started = []
        stopped = []

        class TestComp(Component[EmptyConfig]):
            async def on_start(self):
                started.append(True)

            async def on_stop(self):
                stopped.append(True)

        comp = TestComp(name="test", config=EmptyConfig())
        await comp.on_start()
        await comp.on_stop()

        assert started == [True]
        assert stopped == [True]


# =============================================================================
# TestSenderMixin
# =============================================================================


class TestSenderMixin:
    """Tests for SenderMixin."""

    @pytest.mark.asyncio
    async def test_send_data_creates_message(self, mock_channel_group):
        """Test that send_data creates and sends a DATA message."""

        class TestSender(SenderMixin):
            name = "test_sender"

        sender = TestSender()
        sender._output_channel_group = mock_channel_group

        await sender.send_data({"value": 42})

        mock_channel_group.send.assert_called_once()
        sent_message = mock_channel_group.send.call_args[0][0]
        assert sent_message.message_type == MessageType.DATA
        assert sent_message.payload == {"value": 42}
        assert sent_message.source_component == "test_sender"

    @pytest.mark.asyncio
    async def test_send_data_with_payload_type(self, mock_channel_group):
        """Test that send_data includes payload_type when provided."""

        class TestSender(SenderMixin):
            name = "test_sender"

        sender = TestSender()
        sender._output_channel_group = mock_channel_group

        await sender.send_data({"value": 42}, payload_type="mymodule.MyModel")

        sent_message = mock_channel_group.send.call_args[0][0]
        assert sent_message.payload_type == "mymodule.MyModel"

    @pytest.mark.asyncio
    async def test_send_error_creates_message(self, mock_channel_group):
        """Test that send_error creates and sends an ERROR message."""

        class TestSender(SenderMixin):
            name = "test_sender"

        sender = TestSender()
        sender._output_channel_group = mock_channel_group

        await sender.send_error("Something went wrong")

        sent_message = mock_channel_group.send.call_args[0][0]
        assert sent_message.message_type == MessageType.ERROR
        assert sent_message.payload == "Something went wrong"

    @pytest.mark.asyncio
    async def test_send_error_with_exception(self, mock_channel_group):
        """Test that send_error handles exception objects."""

        class TestSender(SenderMixin):
            name = "test_sender"

        sender = TestSender()
        sender._output_channel_group = mock_channel_group

        await sender.send_error(ValueError("Invalid value"))

        sent_message = mock_channel_group.send.call_args[0][0]
        assert "Invalid value" in sent_message.payload

    @pytest.mark.asyncio
    async def test_send_end_of_stream_creates_message(self, mock_channel_group):
        """Test that send_end_of_stream creates and sends an EOS message."""

        class TestSender(SenderMixin):
            name = "test_sender"

        sender = TestSender()
        sender._output_channel_group = mock_channel_group

        await sender.send_end_of_stream()

        sent_message = mock_channel_group.send.call_args[0][0]
        assert sent_message.message_type == MessageType.END_OF_STREAM
        assert sent_message.payload is None

    @pytest.mark.asyncio
    async def test_send_without_channel_raises(self):
        """Test that sending without channel group raises RuntimeError."""

        class TestSender(SenderMixin):
            name = "test_sender"

        sender = TestSender()
        # _output_channel_group is None by default

        with pytest.raises(RuntimeError, match="no output channel group"):
            await sender.send_data({"value": 42})


# =============================================================================
# TestReceiverMixin
# =============================================================================


class TestReceiverMixin:
    """Tests for ReceiverMixin."""

    @pytest.mark.asyncio
    async def test_listen_and_dispatch_routes_data(self, mock_input_channel):
        """Test that _listen_and_dispatch routes DATA messages correctly."""
        received_data = []

        class TestReceiver(ReceiverMixin):
            name = "test_receiver"

            async def on_received_data(self, message: Message[Any]) -> None:
                received_data.append(message)

        # Set up mock to return DATA then EOS
        data_msg = Message.data({"value": 42}, source_component="source")
        eos_msg = Message.end_of_stream(source_component="source")
        mock_input_channel.receive.side_effect = [data_msg, eos_msg]

        receiver = TestReceiver()
        receiver._input_channel = mock_input_channel

        await receiver._listen_and_dispatch()

        assert len(received_data) == 1
        assert received_data[0].payload == {"value": 42}

    @pytest.mark.asyncio
    async def test_listen_and_dispatch_routes_error(self, mock_input_channel):
        """Test that _listen_and_dispatch routes ERROR messages correctly."""
        received_errors = []

        class TestReceiver(ReceiverMixin):
            name = "test_receiver"

            async def on_received_data(self, message: Message[Any]) -> None:
                pass

            async def on_received_error(self, message: Message[Any]) -> None:
                received_errors.append(message)

        error_msg = Message.error("Something went wrong", source_component="source")
        eos_msg = Message.end_of_stream(source_component="source")
        mock_input_channel.receive.side_effect = [error_msg, eos_msg]

        receiver = TestReceiver()
        receiver._input_channel = mock_input_channel

        await receiver._listen_and_dispatch()

        assert len(received_errors) == 1
        assert "Something went wrong" in received_errors[0].payload

    @pytest.mark.asyncio
    async def test_listen_and_dispatch_exits_on_eos(self, mock_input_channel):
        """Test that _listen_and_dispatch exits on END_OF_STREAM."""
        ending_called = []

        class TestReceiver(ReceiverMixin):
            name = "test_receiver"

            async def on_received_data(self, message: Message[Any]) -> None:
                pass

            async def on_received_ending(self, message: Message[Any]) -> None:
                ending_called.append(True)

        eos_msg = Message.end_of_stream(source_component="source")
        mock_input_channel.receive.side_effect = [eos_msg]

        receiver = TestReceiver()
        receiver._input_channel = mock_input_channel

        await receiver._listen_and_dispatch()

        assert ending_called == [True]
        # Should have exited after EOS
        assert mock_input_channel.receive.call_count == 1

    @pytest.mark.asyncio
    async def test_listen_without_channel_raises(self):
        """Test that listening without input channel raises RuntimeError."""

        class TestReceiver(ReceiverMixin):
            name = "test_receiver"

            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        receiver = TestReceiver()
        # _input_channel is None by default

        with pytest.raises(RuntimeError, match="no input channel"):
            await receiver._listen_and_dispatch()


# =============================================================================
# TestDataProvider
# =============================================================================


class TestDataProvider:
    """Tests for DataProvider class."""

    def test_data_provider_is_triggerable(self):
        """Test that DataProvider implements Triggerable protocol."""

        class TestProvider(DataProvider[SimpleConfig]):
            async def run(self) -> None:
                pass

        assert isinstance(TestProvider, type)
        # Check that instances satisfy Triggerable
        provider = TestProvider(name="test", config=SimpleConfig())
        assert hasattr(provider, "run")
        assert hasattr(provider, "request_stop")
        assert hasattr(provider, "_stop_requested")

    def test_data_provider_stop_requested_default_false(self):
        """Test that _stop_requested is False by default."""

        class TestProvider(DataProvider[SimpleConfig]):
            async def run(self) -> None:
                pass

        provider = TestProvider(name="test", config=SimpleConfig())
        assert provider._stop_requested is False

    def test_request_stop_sets_flag(self):
        """Test that request_stop sets _stop_requested to True."""

        class TestProvider(DataProvider[SimpleConfig]):
            async def run(self) -> None:
                pass

        provider = TestProvider(name="test", config=SimpleConfig())
        provider.request_stop()
        assert provider._stop_requested is True

    @pytest.mark.asyncio
    async def test_data_provider_run_is_abstract(self):
        """Test that DataProvider.run() is abstract."""
        # Can't instantiate without implementing run()
        with pytest.raises(TypeError, match="abstract"):

            class IncompleteProvider(DataProvider[SimpleConfig]):
                pass

            IncompleteProvider(name="test", config=SimpleConfig())

    @pytest.mark.asyncio
    async def test_data_provider_can_send_data(self, mock_channel_group):
        """Test that DataProvider can send data via SenderMixin."""

        class TestProvider(DataProvider[SimpleConfig]):
            async def run(self) -> None:
                await self.send_data({"value": self.config.value})
                await self.send_end_of_stream()

        provider = TestProvider(name="test", config=SimpleConfig(value=42))
        provider._output_channel_group = mock_channel_group

        await provider.run()

        assert mock_channel_group.send.call_count == 2
        first_msg = mock_channel_group.send.call_args_list[0][0][0]
        assert first_msg.payload == {"value": 42}

    def test_data_provider_has_config(self):
        """Test that DataProvider has access to config."""

        class TestProvider(DataProvider[ThresholdConfig]):
            async def run(self) -> None:
                pass

        config = ThresholdConfig(threshold=0.8, enabled=False)
        provider = TestProvider(name="test", config=config)
        assert provider.config.threshold == 0.8
        assert provider.config.enabled is False


# =============================================================================
# TestAlgorithm
# =============================================================================


class TestAlgorithm:
    """Tests for Algorithm class."""

    def test_algorithm_is_not_triggerable(self):
        """Test that Algorithm is not Triggerable (no run method)."""

        class TestAlgo(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        algo = TestAlgo(name="test", config=SimpleConfig())
        assert not isinstance(algo, Triggerable)

    @pytest.mark.asyncio
    async def test_algorithm_on_received_data_is_abstract(self):
        """Test that Algorithm.on_received_data() is abstract."""
        with pytest.raises(TypeError, match="abstract"):

            class IncompleteAlgo(Algorithm[SimpleConfig]):
                pass

            IncompleteAlgo(name="test", config=SimpleConfig())

    def test_algorithm_has_receiver_mixin(self):
        """Test that Algorithm has ReceiverMixin capabilities."""

        class TestAlgo(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        algo = TestAlgo(name="test", config=SimpleConfig())
        assert hasattr(algo, "_input_channel")
        assert hasattr(algo, "_listen_and_dispatch")

    @pytest.mark.asyncio
    async def test_algorithm_processes_data(self, mock_input_channel):
        """Test that Algorithm can process data messages."""
        processed = []

        class TestAlgo(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                processed.append(message.payload)

        data_msg = Message.data({"value": 42}, source_component="source")
        eos_msg = Message.end_of_stream(source_component="source")
        mock_input_channel.receive.side_effect = [data_msg, eos_msg]

        algo = TestAlgo(name="test", config=SimpleConfig())
        algo._input_channel = mock_input_channel

        await algo._listen_and_dispatch()

        assert processed == [{"value": 42}]


# =============================================================================
# TestDecorators
# =============================================================================


class TestDecorators:
    """Tests for @algorithm and @data_provider decorators."""

    def test_algorithm_decorator_registers_class(self):
        """Test that @algorithm decorator registers the class."""
        registry = get_component_registry()

        @algorithm("decorated_algo")
        class DecoratedAlgo(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        assert registry.get_component("decorated_algo") is DecoratedAlgo

    def test_data_provider_decorator_registers_class(self):
        """Test that @data_provider decorator registers the class."""
        registry = get_component_registry()

        @data_provider("decorated_provider")
        class DecoratedProvider(DataProvider[SimpleConfig]):
            async def run(self) -> None:
                pass

        assert registry.get_component("decorated_provider") is DecoratedProvider

    def test_algorithm_decorator_rejects_wrong_base(self):
        """Test that @algorithm rejects non-Algorithm classes."""
        with pytest.raises(TypeError, match="must be a subclass"):

            @algorithm("wrong_algo")
            class WrongAlgo(DataProvider[SimpleConfig]):
                async def run(self) -> None:
                    pass

    def test_data_provider_decorator_rejects_wrong_base(self):
        """Test that @data_provider rejects non-DataProvider classes."""
        with pytest.raises(TypeError, match="must be a subclass"):

            @data_provider("wrong_provider")
            class WrongProvider(Algorithm[SimpleConfig]):
                async def on_received_data(self, message: Message[Any]) -> None:
                    pass

    def test_decorator_preserves_class(self):
        """Test that decorator returns the same class."""

        @algorithm("preserved_algo")
        class PreservedAlgo(Algorithm[SimpleConfig]):
            custom_attr = "test"

            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        assert PreservedAlgo.custom_attr == "test"


# =============================================================================
# TestComponentFactory
# =============================================================================


class TestComponentFactory:
    """Tests for ComponentFactory."""

    def test_create_component_from_registry(self):
        """Test creating a component from registry."""
        registry = get_component_registry()

        @algorithm("factory_algo")
        class FactoryAlgo(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        factory = ComponentFactory()
        component = factory.create_component(
            component_name="factory_algo",
            instance_name="my_algo_instance",
            config_dict={"value": 99, "name": "custom"},
        )

        assert component.name == "my_algo_instance"
        assert component.config.value == 99
        assert component.config.name == "custom"
        assert isinstance(component, FactoryAlgo)

    def test_create_component_with_empty_config(self):
        """Test creating a component with no config provided."""

        @algorithm("empty_config_algo")
        class EmptyConfigAlgo(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        factory = ComponentFactory()
        component = factory.create_component(
            component_name="empty_config_algo",
            instance_name="my_instance",
            config_dict=None,
        )

        # Should use defaults from SimpleConfig
        assert component.config.value == 10
        assert component.config.name == "default"

    def test_create_component_not_found_raises(self):
        """Test that creating nonexistent component raises ComponentNotFoundError."""
        factory = ComponentFactory()

        with pytest.raises(ComponentNotFoundError):
            factory.create_component(
                component_name="nonexistent",
                instance_name="instance",
                config_dict={},
            )

    def test_create_component_invalid_config_raises(self):
        """Test that invalid config raises PipelineConfigError."""

        class StrictConfig(BaseModel):
            required_field: int

        @algorithm("strict_algo")
        class StrictAlgo(Algorithm[StrictConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        factory = ComponentFactory()

        with pytest.raises(PipelineConfigError) as exc_info:
            factory.create_component(
                component_name="strict_algo",
                instance_name="my_instance",
                config_dict={},  # Missing required_field
            )

        assert "Invalid configuration" in str(exc_info.value)

    def test_get_component_class(self):
        """Test getting component class without instantiating."""

        @algorithm("class_only_algo")
        class ClassOnlyAlgo(Algorithm[SimpleConfig]):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        factory = ComponentFactory()
        cls = factory.get_component_class("class_only_algo")
        assert cls is ClassOnlyAlgo


# =============================================================================
# TestProcessorMixin
# =============================================================================


class TestProcessorMixin:
    """Tests for ProcessorMixin (combined send/receive)."""

    def test_processor_mixin_has_both_capabilities(self):
        """Test that ProcessorMixin has both send and receive capabilities."""

        class TestProcessor(ProcessorMixin):
            name = "test"

            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        processor = TestProcessor()
        # From SenderMixin
        assert hasattr(processor, "_output_channel_group")
        assert hasattr(processor, "send_data")
        # From ReceiverMixin
        assert hasattr(processor, "_input_channel")
        assert hasattr(processor, "_listen_and_dispatch")


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Integration tests for the component system."""

    @pytest.mark.asyncio
    async def test_full_pipeline_simulation(self, mock_channel_group, mock_input_channel):
        """Test a simulated pipeline with DataProvider and Algorithm."""

        @data_provider("counter_provider")
        class CounterProvider(DataProvider[SimpleConfig]):
            async def run(self) -> None:
                for i in range(self.config.value):
                    if self._stop_requested:
                        break
                    await self.send_data({"count": i})
                await self.send_end_of_stream()

        @algorithm("accumulator_algo")
        class AccumulatorAlgo(Algorithm[EmptyConfig]):
            def __init__(self, name: str, config: EmptyConfig) -> None:
                super().__init__(name, config)
                self.total = 0

            async def on_received_data(self, message: Message[Any]) -> None:
                self.total += message.payload["count"]

        # Create components via factory
        factory = ComponentFactory()

        provider = factory.create_component(
            "counter_provider", "my_counter", {"value": 5, "name": "counter"}
        )
        algo = factory.create_component("accumulator_algo", "my_accumulator", {})

        # Wire up provider
        provider._output_channel_group = mock_channel_group

        # Run provider
        await provider.run()

        # Verify messages were sent (5 data + 1 EOS)
        assert mock_channel_group.send.call_count == 6

        # Verify data messages
        data_messages = [
            call[0][0]
            for call in mock_channel_group.send.call_args_list
            if call[0][0].message_type == MessageType.DATA
        ]
        assert len(data_messages) == 5
        counts = [msg.payload["count"] for msg in data_messages]
        assert counts == [0, 1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, mock_channel_group):
        """Test graceful shutdown via request_stop."""

        @data_provider("shutdown_provider")
        class ShutdownProvider(DataProvider[EmptyConfig]):
            async def run(self) -> None:
                count = 0
                while not self._stop_requested:
                    await self.send_data({"count": count})
                    count += 1
                    if count >= 100:  # Safety limit
                        break
                    await asyncio.sleep(0.001)
                await self.send_end_of_stream()

        factory = ComponentFactory()
        provider = factory.create_component("shutdown_provider", "my_provider", {})
        provider._output_channel_group = mock_channel_group

        # Start run in background
        task = asyncio.create_task(provider.run())

        # Let it run briefly
        await asyncio.sleep(0.01)

        # Request stop
        provider.request_stop()

        # Wait for completion
        await asyncio.wait_for(task, timeout=1.0)

        # Should have sent some data and EOS
        assert mock_channel_group.send.call_count > 0
        last_msg = mock_channel_group.send.call_args_list[-1][0][0]
        assert last_msg.message_type == MessageType.END_OF_STREAM

    def test_config_class_extraction_across_hierarchy(self):
        """Test config class extraction works through complex inheritance."""

        class BaseAlgo(Algorithm[ThresholdConfig]):
            pass

        @algorithm("derived_algo")
        class DerivedAlgo(BaseAlgo):
            async def on_received_data(self, message: Message[Any]) -> None:
                pass

        config_class = DerivedAlgo.get_config_class()
        assert config_class is ThresholdConfig
