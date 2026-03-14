"""Tests for Vectis examples.

These tests validate that all examples in the examples/ directory
execute correctly and produce expected results.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from vectis import get_component_registry, get_component_type_registry
from vectis.config.loader import ConfigLoader
from vectis.engine.engine import Engine


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def clear_registries():
    """Clear registries before and after each test."""
    get_component_registry().clear()
    get_component_type_registry().clear()
    # Re-register built-in types
    from vectis.components.types import _register_builtin_types

    _register_builtin_types()

    # Clear cached example modules to force re-registration
    modules_to_remove = [
        key for key in sys.modules.keys() if key.startswith("examples.")
    ]
    for mod in modules_to_remove:
        del sys.modules[mod]

    yield
    get_component_registry().clear()


@pytest.fixture
def examples_dir() -> Path:
    """Return the path to the examples directory."""
    return Path(__file__).parent.parent / "examples"


def _register_simple_pipeline_components():
    """Import and register simple_pipeline components."""
    # Force fresh import to trigger registration
    if "examples.simple_pipeline.components" in sys.modules:
        importlib.reload(sys.modules["examples.simple_pipeline.components"])
    else:
        import examples.simple_pipeline.components  # noqa: F401


def _register_etl_pipeline_components():
    """Import and register etl_pipeline components."""
    if "examples.etl_pipeline.components" in sys.modules:
        importlib.reload(sys.modules["examples.etl_pipeline.components"])
    else:
        import examples.etl_pipeline.components  # noqa: F401


def _register_distributed_components():
    """Import and register distributed_example components."""
    if "examples.distributed_example.components" in sys.modules:
        importlib.reload(sys.modules["examples.distributed_example.components"])
    else:
        import examples.distributed_example.components  # noqa: F401


def _register_custom_component_type_components():
    """Import and register custom_component_type components."""
    if "examples.custom_component_type.components" in sys.modules:
        importlib.reload(sys.modules["examples.custom_component_type.components"])
    else:
        import examples.custom_component_type.components  # noqa: F401


# =============================================================================
# Simple Pipeline Example Tests
# =============================================================================


class TestSimplePipelineExample:
    """Tests for the simple_pipeline example."""

    def test_components_importable(self):
        """Test that simple_pipeline components can be imported."""
        _register_simple_pipeline_components()
        from examples.simple_pipeline.components import (
            CounterConfig,
            CounterProvider,
            PrinterAlgorithm,
        )

        assert CounterProvider is not None
        assert PrinterAlgorithm is not None
        assert CounterConfig is not None

    def test_components_registered(self):
        """Test that importing components registers them."""
        _register_simple_pipeline_components()

        registry = get_component_registry()
        assert "simple_counter" in registry.components
        assert "simple_printer" in registry.components

    def test_yaml_config_valid(self, examples_dir):
        """Test that simple_pipeline YAML config is valid."""
        _register_simple_pipeline_components()

        config_path = examples_dir / "simple_pipeline" / "pipeline.yaml"
        assert config_path.exists(), f"Config not found: {config_path}"

        loader = ConfigLoader()
        config = loader.load(str(config_path))

        assert config.global_config.name == "simple-pipeline"
        assert len(config.connections) == 1
        assert config.connections[0].source == "counter"
        assert "printer" in config.connections[0].targets

    @pytest.mark.asyncio
    async def test_simple_pipeline_executes(self, examples_dir):
        """Test that simple_pipeline executes and produces correct output."""
        _register_simple_pipeline_components()

        config_path = examples_dir / "simple_pipeline" / "pipeline.yaml"
        engine = Engine(str(config_path))

        await engine.run()

        # Verify counter sent correct values
        counter = engine.components["counter"]
        assert counter.sent_values == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        # Verify printer received all values
        printer = engine.components["printer"]
        assert printer.received_count == 10
        assert len(printer.received_values) == 10

        # Verify payload structure
        values = [v["value"] for v in printer.received_values]
        assert values == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    @pytest.mark.asyncio
    async def test_simple_pipeline_custom_config(self, tmp_path):
        """Test simple_pipeline with custom configuration."""
        _register_simple_pipeline_components()

        yaml_content = """
global:
  name: custom-simple

data_providers:
  - name: counter
    type: simple_counter
    config:
      count: 5
      start: 100

algorithms:
  - name: printer
    type: simple_printer

connections:
  - source: counter
    targets: [printer]
"""
        config_file = tmp_path / "custom.yaml"
        config_file.write_text(yaml_content)

        engine = Engine(str(config_file))
        await engine.run()

        printer = engine.components["printer"]
        values = [v["value"] for v in printer.received_values]
        assert values == [100, 101, 102, 103, 104]


# =============================================================================
# ETL Pipeline Example Tests
# =============================================================================


class TestETLPipelineExample:
    """Tests for the etl_pipeline example."""

    def test_components_importable(self):
        """Test that etl_pipeline components can be imported."""
        _register_etl_pipeline_components()
        from examples.etl_pipeline.components import (
            DataSource,
            Transformer,
            Loader,
        )

        assert DataSource is not None
        assert Transformer is not None
        assert Loader is not None

    def test_components_registered(self):
        """Test that importing components registers them."""
        _register_etl_pipeline_components()

        registry = get_component_registry()
        assert "etl_data_source" in registry.components
        assert "etl_transformer" in registry.components
        assert "etl_loader" in registry.components

    def test_yaml_config_valid(self, examples_dir):
        """Test that etl_pipeline YAML config is valid."""
        _register_etl_pipeline_components()

        config_path = examples_dir / "etl_pipeline" / "pipeline.yaml"
        assert config_path.exists(), f"Config not found: {config_path}"

        loader = ConfigLoader()
        config = loader.load(str(config_path))

        assert config.global_config.name == "etl-pipeline"

    @pytest.mark.asyncio
    async def test_etl_pipeline_executes(self, examples_dir):
        """Test that etl_pipeline executes correctly."""
        _register_etl_pipeline_components()

        config_path = examples_dir / "etl_pipeline" / "pipeline.yaml"
        engine = Engine(str(config_path))

        await engine.run()

        # Verify data flowed through the pipeline
        loader_comp = engine.components["loader"]
        assert loader_comp.loaded_count > 0


# =============================================================================
# Distributed Example Tests
# =============================================================================


class TestDistributedExample:
    """Tests for the distributed_example."""

    def test_components_importable(self):
        """Test that distributed_example components can be imported."""
        _register_distributed_components()
        from examples.distributed_example.components import (
            DistributedProducer,
            DistributedConsumer,
        )

        assert DistributedProducer is not None
        assert DistributedConsumer is not None

    def test_components_registered(self):
        """Test that importing components registers them."""
        _register_distributed_components()

        registry = get_component_registry()
        assert "distributed_producer" in registry.components
        assert "distributed_consumer" in registry.components

    @pytest.mark.asyncio
    async def test_distributed_with_force_inprocess(self, examples_dir):
        """Test distributed example runs locally with force_inprocess."""
        _register_distributed_components()

        config_path = examples_dir / "distributed_example" / "pipeline.yaml"
        engine = Engine(str(config_path))

        # Run with force_inprocess to test locally
        await engine.run(force_inprocess=True)

        # Verify data was processed
        consumer = engine.components.get("consumer1") or engine.components.get(
            "consumer"
        )
        if consumer:
            assert consumer.processed_count > 0


# =============================================================================
# Custom Component Type Example Tests
# =============================================================================


class TestCustomComponentTypeExample:
    """Tests for the custom_component_type example."""

    def test_components_importable(self):
        """Test that custom_component_type components can be imported."""
        _register_custom_component_type_components()
        from examples.custom_component_type.components import (  # noqa: F401
            CounterProvider,
            MultiplierProcessor,
            PrinterAlgorithm,
            Processor,
        )

        assert CounterProvider is not None
        assert MultiplierProcessor is not None
        assert PrinterAlgorithm is not None
        assert Processor is not None

    def test_components_registered(self):
        """Test that importing components registers them."""
        _register_custom_component_type_components()

        registry = get_component_registry()
        assert "custom_counter" in registry.components
        assert "value_multiplier" in registry.components
        assert "custom_printer" in registry.components

        type_registry = get_component_type_registry()
        assert "processor" in type_registry.types

    def test_yaml_config_valid(self, examples_dir):
        """Test that custom_component_type YAML config is valid."""
        _register_custom_component_type_components()

        config_path = examples_dir / "custom_component_type" / "pipeline.yaml"
        assert config_path.exists(), f"Config not found: {config_path}"

        loader = ConfigLoader()
        config = loader.load(str(config_path))

        assert config.global_config.name == "custom-component-type"
        assert len(config.connections) == 2
        assert config.connections[0].source == "counter"
        assert "multiplier" in config.connections[0].targets

    @pytest.mark.asyncio
    async def test_custom_component_type_executes(self, examples_dir):
        """Test that custom_component_type executes and produces expected output."""
        _register_custom_component_type_components()

        config_path = examples_dir / "custom_component_type" / "pipeline.yaml"
        engine = Engine(str(config_path))

        await engine.run()

        multiplier = engine.components["multiplier"]
        assert multiplier.processed_values == [3, 6, 9, 12, 15]

        printer = engine.components["printer"]
        values = [v["value"] for v in printer.received_values]
        assert values == [3, 6, 9, 12, 15]
