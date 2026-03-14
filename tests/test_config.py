"""Tests for Vectis Phase 4: Configuration System.

This module tests:
- Pydantic configuration models
- ConfigLoader YAML parsing
- Validation against registry and topology
- Defaults propagation
- Error handling for invalid configs
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from vectis.communication.enums import (
    BackpressureMode,
    CompetingStrategy,
    DistributionMode,
    StartupSyncStrategy,
)
from vectis.config import (
    BackpressureConfig,
    ComponentInstanceConfig,
    ConfigLoader,
    ConnectionConfig,
    DefaultsConfig,
    GlobalConfig,
    PipelineConfig,
    TransportConfig,
    WorkerConfig,
)
from vectis.exceptions import PipelineConfigError


# =============================================================================
# Fixtures
# =============================================================================

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def loader() -> ConfigLoader:
    """Create a ConfigLoader instance."""
    return ConfigLoader()


@pytest.fixture
def simple_yaml_content() -> str:
    """Simple valid YAML configuration."""
    return """
global:
  name: test_pipeline
  version: "1.0"

algorithms:
  - name: algo_1
    type: test_algorithm
    config: {}

data_providers:
  - name: provider_1
    type: test_provider
    config: {}

connections:
  - source: provider_1
    targets: [algo_1]
"""


# =============================================================================
# BackpressureConfig Tests
# =============================================================================


class TestBackpressureConfig:
    """Tests for BackpressureConfig model."""

    def test_default_values(self) -> None:
        """Test default values are applied correctly."""
        config = BackpressureConfig()
        assert config.mode == BackpressureMode.BLOCK
        assert config.queue_size == 1000

    def test_string_mode_parsing_block(self) -> None:
        """Test that string 'block' is parsed to enum."""
        config = BackpressureConfig(mode="block")
        assert config.mode == BackpressureMode.BLOCK

    def test_string_mode_parsing_drop(self) -> None:
        """Test that string 'drop' is parsed to enum."""
        config = BackpressureConfig(mode="drop")
        assert config.mode == BackpressureMode.DROP

    def test_string_mode_case_insensitive(self) -> None:
        """Test that mode parsing is case-insensitive."""
        config = BackpressureConfig(mode="DROP")
        assert config.mode == BackpressureMode.DROP

    def test_invalid_mode_raises(self) -> None:
        """Test that invalid mode raises ValidationError."""
        with pytest.raises(ValidationError):
            BackpressureConfig(mode="invalid")

    def test_queue_size_must_be_positive(self) -> None:
        """Test that queue_size must be at least 1."""
        with pytest.raises(ValidationError):
            BackpressureConfig(queue_size=0)

    def test_extra_fields_forbidden(self) -> None:
        """Test that extra fields raise ValidationError."""
        with pytest.raises(ValidationError):
            BackpressureConfig(mode="block", extra_field="value")


# =============================================================================
# DefaultsConfig Tests
# =============================================================================


class TestDefaultsConfig:
    """Tests for DefaultsConfig model."""

    def test_default_values(self) -> None:
        """Test default values are applied correctly."""
        config = DefaultsConfig()
        assert config.serialization == "json"
        assert config.distribution == DistributionMode.FAN_OUT
        assert config.strategy == CompetingStrategy.ROUND_ROBIN
        assert config.backpressure.mode == BackpressureMode.BLOCK

    def test_distribution_string_parsing_fan_out(self) -> None:
        """Test that string 'fan_out' is parsed to enum."""
        config = DefaultsConfig(distribution="fan_out")
        assert config.distribution == DistributionMode.FAN_OUT

    def test_distribution_string_parsing_hyphen(self) -> None:
        """Test that string 'fan-out' is parsed to enum."""
        config = DefaultsConfig(distribution="fan-out")
        assert config.distribution == DistributionMode.FAN_OUT

    def test_distribution_competing(self) -> None:
        """Test that competing distribution is parsed."""
        config = DefaultsConfig(distribution="competing")
        assert config.distribution == DistributionMode.COMPETING

    def test_strategy_string_parsing(self) -> None:
        """Test that strategy strings are parsed."""
        config = DefaultsConfig(strategy="round_robin")
        assert config.strategy == CompetingStrategy.ROUND_ROBIN

    def test_strategy_random(self) -> None:
        """Test random strategy parsing."""
        config = DefaultsConfig(strategy="random")
        assert config.strategy == CompetingStrategy.RANDOM

    def test_invalid_serialization_raises(self) -> None:
        """Test that invalid serialization raises ValidationError."""
        with pytest.raises(ValidationError):
            DefaultsConfig(serialization="invalid")

    def test_nested_backpressure(self) -> None:
        """Test that nested backpressure config works."""
        config = DefaultsConfig(
            backpressure=BackpressureConfig(mode="drop", queue_size=500)
        )
        assert config.backpressure.mode == BackpressureMode.DROP
        assert config.backpressure.queue_size == 500


# =============================================================================
# TransportConfig Tests
# =============================================================================


class TestTransportConfig:
    """Tests for TransportConfig model."""

    def test_required_type(self) -> None:
        """Test that type is required."""
        with pytest.raises(ValidationError):
            TransportConfig()

    def test_config_defaults_to_empty(self) -> None:
        """Test that config defaults to empty dict."""
        config = TransportConfig(type="zeromq")
        assert config.type == "zeromq"
        assert config.config == {}

    def test_with_config(self) -> None:
        """Test with additional config."""
        config = TransportConfig(type="zeromq", config={"port": 5555})
        assert config.config["port"] == 5555


# =============================================================================
# WorkerConfig Tests
# =============================================================================


class TestWorkerConfig:
    """Tests for WorkerConfig model."""

    def test_name_required(self) -> None:
        """Test that name is required."""
        with pytest.raises(ValidationError):
            WorkerConfig()

    def test_host_defaults_to_localhost(self) -> None:
        """Test that host defaults to localhost."""
        config = WorkerConfig(name="worker_1")
        assert config.host == "localhost"

    def test_with_custom_host(self) -> None:
        """Test with custom host."""
        config = WorkerConfig(name="worker_1", host="192.168.1.100")
        assert config.host == "192.168.1.100"

    def test_empty_name_raises(self) -> None:
        """Test that empty name raises ValidationError."""
        with pytest.raises(ValidationError):
            WorkerConfig(name="")


# =============================================================================
# ComponentInstanceConfig Tests
# =============================================================================


class TestComponentInstanceConfig:
    """Tests for ComponentInstanceConfig model."""

    def test_name_required(self) -> None:
        """Test that name is required."""
        with pytest.raises(ValidationError):
            ComponentInstanceConfig(type="some_type")

    def test_type_required(self) -> None:
        """Test that type is required."""
        with pytest.raises(ValidationError):
            ComponentInstanceConfig(name="my_algo")

    def test_defaults(self) -> None:
        """Test default values."""
        config = ComponentInstanceConfig(name="my_algo", type="algorithm_impl")
        assert config.name == "my_algo"
        assert config.type == "algorithm_impl"
        assert config.worker is None
        assert config.config == {}

    def test_with_worker(self) -> None:
        """Test with worker assignment."""
        config = ComponentInstanceConfig(
            name="my_algo", type="algorithm_impl", worker="worker_1"
        )
        assert config.worker == "worker_1"

    def test_with_config(self) -> None:
        """Test with component-specific config."""
        config = ComponentInstanceConfig(
            name="my_algo",
            type="algorithm_impl",
            config={"threshold": 0.5, "max_items": 100},
        )
        assert config.config["threshold"] == 0.5

    def test_empty_type_raises(self) -> None:
        """Test that empty type raises ValidationError."""
        with pytest.raises(ValidationError):
            ComponentInstanceConfig(name="my_algo", type="")

    def test_empty_name_raises(self) -> None:
        """Test that empty name raises ValidationError."""
        with pytest.raises(ValidationError):
            ComponentInstanceConfig(name="", type="some_type")


# =============================================================================
# ConnectionConfig Tests
# =============================================================================


class TestConnectionConfig:
    """Tests for ConnectionConfig model."""

    def test_required_fields(self) -> None:
        """Test that source and targets are required."""
        with pytest.raises(ValidationError):
            ConnectionConfig()

    def test_targets_must_not_be_empty(self) -> None:
        """Test that targets must have at least one element."""
        with pytest.raises(ValidationError):
            ConnectionConfig(source="provider", targets=[])

    def test_defaults(self) -> None:
        """Test default values (None for optional overrides)."""
        config = ConnectionConfig(source="provider", targets=["algo"])
        assert config.distribution is None
        assert config.strategy is None
        assert config.serialization is None
        assert config.backpressure is None

    def test_with_overrides(self) -> None:
        """Test with all overrides specified."""
        config = ConnectionConfig(
            source="provider",
            targets=["algo1", "algo2"],
            distribution="competing",
            strategy="random",
            serialization="msgpack",
            backpressure=BackpressureConfig(mode="drop", queue_size=100),
        )
        assert config.distribution == DistributionMode.COMPETING
        assert config.strategy == CompetingStrategy.RANDOM
        assert config.serialization == "msgpack"
        assert config.backpressure.mode == BackpressureMode.DROP

    def test_distribution_hyphen_parsing(self) -> None:
        """Test that 'fan-out' is parsed correctly."""
        config = ConnectionConfig(
            source="provider", targets=["algo"], distribution="fan-out"
        )
        assert config.distribution == DistributionMode.FAN_OUT

    def test_ports_default_none(self) -> None:
        """Test that ports defaults to None."""
        config = ConnectionConfig(source="provider", targets=["algo"])
        assert config.ports is None

    def test_ports_valid(self) -> None:
        """Test that valid port mapping is accepted."""
        config = ConnectionConfig(
            source="provider",
            targets=["algo1", "algo2"],
            ports={"algo1": 6000, "algo2": 6100},
        )
        assert config.ports == {"algo1": 6000, "algo2": 6100}

    def test_ports_invalid_range_low(self) -> None:
        """Test that port < 1 raises ValidationError."""
        with pytest.raises(ValidationError, match="must be between 1 and 65535"):
            ConnectionConfig(
                source="provider",
                targets=["algo"],
                ports={"algo": 0},
            )

    def test_ports_invalid_range_high(self) -> None:
        """Test that port > 65535 raises ValidationError."""
        with pytest.raises(ValidationError, match="must be between 1 and 65535"):
            ConnectionConfig(
                source="provider",
                targets=["algo"],
                ports={"algo": 65536},
            )

    def test_ports_partial_coverage(self) -> None:
        """Test that subset of targets with ports works."""
        config = ConnectionConfig(
            source="provider",
            targets=["algo1", "algo2", "algo3"],
            ports={"algo1": 6000},  # Only algo1 has fixed port
        )
        assert config.ports == {"algo1": 6000}

    def test_ports_empty_dict_accepted(self) -> None:
        """Test that empty ports dict is accepted (treated as no overrides)."""
        config = ConnectionConfig(
            source="provider",
            targets=["algo"],
            ports={},
        )
        assert config.ports == {}


# =============================================================================
# GlobalConfig Tests
# =============================================================================


class TestGlobalConfig:
    """Tests for GlobalConfig model."""

    def test_name_required(self) -> None:
        """Test that name is required."""
        with pytest.raises(ValidationError):
            GlobalConfig()

    def test_defaults(self) -> None:
        """Test default values."""
        config = GlobalConfig(name="my_pipeline")
        assert config.version == "1.0"
        assert config.defaults.serialization == "json"
        assert config.transport is None
        assert config.sync_strategy == StartupSyncStrategy.RETRY_BACKOFF

    def test_sync_strategy_string_parsing(self) -> None:
        """Test that sync_strategy accepts strings."""
        config = GlobalConfig(name="my_pipeline", sync_strategy="retry_backoff")
        assert config.sync_strategy == StartupSyncStrategy.RETRY_BACKOFF

    def test_sync_strategy_control_channel(self) -> None:
        """Test control_channel sync strategy."""
        config = GlobalConfig(name="my_pipeline", sync_strategy="control_channel")
        assert config.sync_strategy == StartupSyncStrategy.CONTROL_CHANNEL

    def test_with_transport(self) -> None:
        """Test with transport configuration."""
        config = GlobalConfig(
            name="my_pipeline", transport=TransportConfig(type="zeromq")
        )
        assert config.transport.type == "zeromq"


# =============================================================================
# PipelineConfig Tests
# =============================================================================


class TestPipelineConfig:
    """Tests for PipelineConfig model."""

    def test_global_config_required(self) -> None:
        """Test that global_config is required."""
        with pytest.raises(ValidationError):
            PipelineConfig()

    def test_defaults(self) -> None:
        """Test default values."""
        config = PipelineConfig(global_config=GlobalConfig(name="test"))
        assert config.workers == []
        assert config.connections == []
        assert config.components_by_type == {}

    def test_get_all_component_instances(self) -> None:
        """Test get_all_component_instances helper method."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo"),
                    ComponentInstanceConfig(name="algo_2", type="test_algo"),
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        instances = config.get_all_component_instances()
        assert len(instances) == 3
        assert "algo_1" in instances
        assert "algo_2" in instances
        assert "provider_1" in instances

    def test_get_component_type(self) -> None:
        """Test get_component_type helper method."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo")
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        assert config.get_component_type("algo_1") == "algorithm"
        assert config.get_component_type("provider_1") == "data_provider"
        assert config.get_component_type("nonexistent") is None


# =============================================================================
# ConfigLoader Parsing Tests
# =============================================================================


class TestConfigLoaderParsing:
    """Tests for ConfigLoader YAML parsing."""

    def test_load_from_string_simple(
        self, loader: ConfigLoader, simple_yaml_content: str
    ) -> None:
        """Test loading from a simple YAML string."""
        config = loader.load_from_string(simple_yaml_content)

        assert config.global_config.name == "test_pipeline"
        assert config.global_config.version == "1.0"
        assert "algorithm" in config.components_by_type
        assert "data_provider" in config.components_by_type
        assert len(config.connections) == 1

    def test_load_from_string_missing_global(self, loader: ConfigLoader) -> None:
        """Test that missing global section raises error."""
        yaml_content = """
algorithms:
  - name: algo_1
    type: test_algo
    config: {}
"""
        with pytest.raises(PipelineConfigError, match="missing required 'global'"):
            loader.load_from_string(yaml_content)

    def test_load_from_string_invalid_yaml(self, loader: ConfigLoader) -> None:
        """Test that invalid YAML raises error."""
        yaml_content = "invalid: yaml: content:"
        with pytest.raises(PipelineConfigError, match="Invalid YAML"):
            loader.load_from_string(yaml_content)

    def test_load_from_string_not_mapping(self, loader: ConfigLoader) -> None:
        """Test that non-mapping YAML raises error."""
        yaml_content = "- item1\n- item2"
        with pytest.raises(PipelineConfigError, match="must be a YAML mapping"):
            loader.load_from_string(yaml_content)

    def test_load_file(self, loader: ConfigLoader) -> None:
        """Test loading from a file."""
        config = loader.load(FIXTURES_DIR / "valid_simple.yaml")

        assert config.global_config.name == "simple_pipeline"
        assert len(config.connections) == 1

    def test_load_file_not_found(self, loader: ConfigLoader) -> None:
        """Test that FileNotFoundError is wrapped."""
        with pytest.raises(PipelineConfigError, match="not found"):
            loader.load("/nonexistent/path/config.yaml")

    def test_load_fanout_config(self, loader: ConfigLoader) -> None:
        """Test loading fan-out configuration."""
        config = loader.load(FIXTURES_DIR / "valid_fanout.yaml")

        assert config.global_config.name == "fanout_pipeline"
        assert len(config.components_by_type.get("algorithm", [])) == 3
        assert config.connections[0].distribution == DistributionMode.FAN_OUT

    def test_load_competing_config(self, loader: ConfigLoader) -> None:
        """Test loading competing configuration."""
        config = loader.load(FIXTURES_DIR / "valid_competing.yaml")

        assert config.global_config.name == "competing_pipeline"
        assert config.connections[0].distribution == DistributionMode.COMPETING
        assert config.connections[0].strategy == CompetingStrategy.ROUND_ROBIN

    def test_load_with_workers(self, loader: ConfigLoader) -> None:
        """Test loading configuration with workers."""
        config = loader.load(FIXTURES_DIR / "valid_with_workers.yaml")

        assert len(config.workers) == 2
        assert config.workers[0].name == "data_worker"
        assert config.workers[1].name == "processing_worker"
        assert config.workers[1].config.get("threads") == 4

    def test_plural_to_singular_normalization(
        self, loader: ConfigLoader, simple_yaml_content: str
    ) -> None:
        """Test that plural section names are normalized to singular."""
        config = loader.load_from_string(simple_yaml_content)

        # 'algorithms' in YAML should become 'algorithm'
        assert "algorithm" in config.components_by_type
        # 'data_providers' should become 'data_provider'
        assert "data_provider" in config.components_by_type


# =============================================================================
# ConfigLoader Validation Tests
# =============================================================================


class TestConfigLoaderValidation:
    """Tests for ConfigLoader validation."""

    def test_validate_valid_config(self, loader: ConfigLoader) -> None:
        """Test that valid config passes validation."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(source="provider_1", targets=["algo_1"])
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo")
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) == 0

    def test_validate_missing_source(self, loader: ConfigLoader) -> None:
        """Test that missing source is detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(source="nonexistent", targets=["algo_1"])
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo")
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any("nonexistent" in e and "not found" in e for e in errors)

    def test_validate_missing_target(self, loader: ConfigLoader) -> None:
        """Test that missing target is detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(source="provider_1", targets=["nonexistent"])
            ],
            components_by_type={
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any("nonexistent" in e and "not found" in e for e in errors)

    def test_validate_self_loop(self, loader: ConfigLoader) -> None:
        """Test that self-loop is detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(source="algo_1", targets=["algo_1"])
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo")
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any("cannot target itself" in e for e in errors)

    def test_validate_duplicate_instance_names(self, loader: ConfigLoader) -> None:
        """Test that duplicate instance names are detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="duplicate", type="test_algo")
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="duplicate", type="test_provider")
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any("Duplicate" in e and "duplicate" in e for e in errors)

    def test_validate_duplicate_worker_names(self, loader: ConfigLoader) -> None:
        """Test that duplicate worker names are detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            workers=[
                WorkerConfig(name="worker_1"),
                WorkerConfig(name="worker_1"),
            ],
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any("Duplicate worker" in e for e in errors)

    def test_validate_unknown_worker_reference(self, loader: ConfigLoader) -> None:
        """Test that unknown worker reference is detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            workers=[WorkerConfig(name="worker_1")],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(
                        name="algo_1", type="test_algo", worker="nonexistent_worker"
                    )
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any("unknown worker" in e.lower() for e in errors)

    def test_validate_from_fixture_invalid_missing(
        self, loader: ConfigLoader
    ) -> None:
        """Test validation of invalid_missing_component fixture."""
        config = loader.load(FIXTURES_DIR / "invalid_missing_component.yaml")
        errors = loader.validate(config)

        assert len(errors) >= 1
        assert any("nonexistent_provider" in e for e in errors)

    def test_validate_from_fixture_invalid_topology(
        self, loader: ConfigLoader
    ) -> None:
        """Test validation of invalid_topology fixture (self-loop)."""
        config = loader.load(FIXTURES_DIR / "invalid_topology.yaml")
        errors = loader.validate(config)

        assert len(errors) >= 1
        assert any("cannot target itself" in e for e in errors)

    def test_validate_from_fixture_invalid_worker(
        self, loader: ConfigLoader
    ) -> None:
        """Test validation of invalid_unknown_worker fixture."""
        config = loader.load(FIXTURES_DIR / "invalid_unknown_worker.yaml")
        errors = loader.validate(config)

        assert len(errors) >= 1
        assert any("nonexistent_worker" in e for e in errors)

    def test_validate_conflicting_distribution_same_source(
        self, loader: ConfigLoader
    ) -> None:
        """Test that conflicting distribution settings on same source are detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_1", "algo_2"],
                    distribution=DistributionMode.FAN_OUT,
                ),
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_3"],
                    distribution=DistributionMode.COMPETING,
                ),
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo"),
                    ComponentInstanceConfig(name="algo_2", type="test_algo"),
                    ComponentInstanceConfig(name="algo_3", type="test_algo"),
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any("conflicting" in e.lower() and "provider_1" in e for e in errors)

    def test_validate_conflicting_strategy_same_source(
        self, loader: ConfigLoader
    ) -> None:
        """Test that conflicting strategy settings on same source are detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_1"],
                    distribution=DistributionMode.COMPETING,
                    strategy=CompetingStrategy.ROUND_ROBIN,
                ),
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_2"],
                    distribution=DistributionMode.COMPETING,
                    strategy=CompetingStrategy.RANDOM,
                ),
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo"),
                    ComponentInstanceConfig(name="algo_2", type="test_algo"),
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any("conflicting" in e.lower() and "provider_1" in e for e in errors)

    def test_validate_same_distribution_same_source_is_valid(
        self, loader: ConfigLoader
    ) -> None:
        """Test that same distribution settings on same source are allowed."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_1", "algo_2"],
                    distribution=DistributionMode.FAN_OUT,
                ),
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_3"],
                    distribution=DistributionMode.FAN_OUT,
                ),
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo"),
                    ComponentInstanceConfig(name="algo_2", type="test_algo"),
                    ComponentInstanceConfig(name="algo_3", type="test_algo"),
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        errors = loader.validate(config)
        # No conflicting distribution errors
        assert not any("conflicting" in e.lower() for e in errors)

    def test_validate_conflicting_uses_effective_settings_with_defaults(
        self, loader: ConfigLoader
    ) -> None:
        """Test that conflict detection uses effective settings (including defaults)."""
        # First connection uses default (fan_out), second explicitly sets competing
        config = PipelineConfig(
            global_config=GlobalConfig(
                name="test",
                defaults=DefaultsConfig(distribution=DistributionMode.FAN_OUT),
            ),
            connections=[
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_1"],
                    # distribution=None -> uses default (fan_out)
                ),
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_2"],
                    distribution=DistributionMode.COMPETING,  # Conflicts with default
                ),
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo"),
                    ComponentInstanceConfig(name="algo_2", type="test_algo"),
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any("conflicting" in e.lower() and "provider_1" in e for e in errors)

    def test_validate_different_sources_can_have_different_distribution(
        self, loader: ConfigLoader
    ) -> None:
        """Test that different sources can have different distribution settings."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_1"],
                    distribution=DistributionMode.FAN_OUT,
                ),
                ConnectionConfig(
                    source="provider_2",
                    targets=["algo_2"],
                    distribution=DistributionMode.COMPETING,
                ),
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo"),
                    ComponentInstanceConfig(name="algo_2", type="test_algo"),
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider"),
                    ComponentInstanceConfig(name="provider_2", type="test_provider"),
                ],
            },
        )

        errors = loader.validate(config)
        # No conflicting distribution errors (different sources)
        assert not any("conflicting" in e.lower() for e in errors)

    def test_validate_port_key_not_in_targets(self, loader: ConfigLoader) -> None:
        """Test that port key not in targets list is detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_1", "algo_2"],
                    ports={"algo_3": 6000},  # algo_3 is not in targets
                )
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo"),
                    ComponentInstanceConfig(name="algo_2", type="test_algo"),
                    ComponentInstanceConfig(name="algo_3", type="test_algo"),
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any(
            "algo_3" in e and "not in targets" in e for e in errors
        )

    def test_validate_duplicate_fixed_ports(self, loader: ConfigLoader) -> None:
        """Test that duplicate fixed ports across connections are detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_1"],
                    ports={"algo_1": 6000},
                ),
                ConnectionConfig(
                    source="provider_2",
                    targets=["algo_2"],
                    ports={"algo_2": 6000},  # Same port as above
                ),
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo"),
                    ComponentInstanceConfig(name="algo_2", type="test_algo"),
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider"),
                    ComponentInstanceConfig(name="provider_2", type="test_provider"),
                ],
            },
        )

        errors = loader.validate(config)
        assert len(errors) >= 1
        assert any("port 6000" in e and "conflicts" in e for e in errors)

    def test_validate_ports_valid_config(self, loader: ConfigLoader) -> None:
        """Test that valid ports configuration passes validation."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            connections=[
                ConnectionConfig(
                    source="provider_1",
                    targets=["algo_1", "algo_2", "algo_3"],
                    ports={"algo_1": 6000, "algo_2": 6100},  # algo_3 uses hash
                )
            ],
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo"),
                    ComponentInstanceConfig(name="algo_2", type="test_algo"),
                    ComponentInstanceConfig(name="algo_3", type="test_algo"),
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        errors = loader.validate(config)
        # Should not have any port-related errors
        assert not any("port" in e.lower() for e in errors)


# =============================================================================
# ConfigLoader Registry Validation Tests
# =============================================================================


class TestConfigLoaderRegistryValidation:
    """Tests for ConfigLoader registry-based validation."""

    def test_validate_with_registry_valid_types(self, loader: ConfigLoader) -> None:
        """Test that valid component types pass registry validation."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(name="algo_1", type="test_algo")
                ],
                "data_provider": [
                    ComponentInstanceConfig(name="provider_1", type="test_provider")
                ],
            },
        )

        errors = loader.validate_with_registry(config)
        # No errors for unknown section types because algorithm and data_provider are registered
        assert not any("Unknown component type" in e for e in errors)

    def test_validate_with_registry_unknown_section_type(
        self, loader: ConfigLoader
    ) -> None:
        """Test that unknown component section type is detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            components_by_type={
                "unknown_type": [
                    ComponentInstanceConfig(name="instance_1", type="some_impl")
                ],
            },
        )

        errors = loader.validate_with_registry(config)
        assert len(errors) >= 1
        assert any("Unknown component type" in e and "unknown_type" in e for e in errors)

    def test_validate_with_registry_unknown_instance_type(
        self, loader: ConfigLoader
    ) -> None:
        """Test that unknown component instance type is detected."""
        config = PipelineConfig(
            global_config=GlobalConfig(name="test"),
            components_by_type={
                "algorithm": [
                    ComponentInstanceConfig(
                        name="algo_1", type="nonexistent_component"
                    )
                ],
            },
        )

        errors = loader.validate_with_registry(config)
        assert len(errors) >= 1
        assert any(
            "nonexistent_component" in e and "unknown type" in e.lower()
            for e in errors
        )


# =============================================================================
# Defaults Propagation Tests
# =============================================================================


class TestDefaultsPropagation:
    """Tests for defaults propagation logic."""

    def test_connection_inherits_none_for_defaults(self) -> None:
        """Test that connection-level overrides are None when not specified."""
        config = PipelineConfig(
            global_config=GlobalConfig(
                name="test",
                defaults=DefaultsConfig(
                    distribution=DistributionMode.COMPETING,
                    strategy=CompetingStrategy.RANDOM,
                ),
            ),
            connections=[
                ConnectionConfig(source="a", targets=["b"])
            ],
            components_by_type={
                "data_provider": [
                    ComponentInstanceConfig(name="a", type="test_provider")
                ],
                "algorithm": [
                    ComponentInstanceConfig(name="b", type="test_algo")
                ],
            },
        )

        conn = config.connections[0]
        # Connection-level overrides should be None (not overridden)
        assert conn.distribution is None
        assert conn.strategy is None
        # The engine would apply defaults from global_config when conn.* is None

    def test_connection_override_takes_precedence(self) -> None:
        """Test that connection-level overrides take precedence."""
        config = PipelineConfig(
            global_config=GlobalConfig(
                name="test",
                defaults=DefaultsConfig(
                    distribution=DistributionMode.FAN_OUT,
                    serialization="json",
                ),
            ),
            connections=[
                ConnectionConfig(
                    source="a",
                    targets=["b"],
                    distribution=DistributionMode.COMPETING,
                    serialization="msgpack",
                )
            ],
            components_by_type={
                "data_provider": [
                    ComponentInstanceConfig(name="a", type="test_provider")
                ],
                "algorithm": [
                    ComponentInstanceConfig(name="b", type="test_algo")
                ],
            },
        )

        conn = config.connections[0]
        # Connection-level overrides should be set
        assert conn.distribution == DistributionMode.COMPETING
        assert conn.serialization == "msgpack"


# =============================================================================
# Integration Tests
# =============================================================================


class TestConfigIntegration:
    """Integration tests for configuration system."""

    def test_full_pipeline_config_from_yaml(self, loader: ConfigLoader) -> None:
        """Test loading and validating a full pipeline configuration."""
        config = loader.load(FIXTURES_DIR / "valid_with_workers.yaml")
        errors = loader.validate(config)

        assert len(errors) == 0
        assert config.global_config.name == "distributed_pipeline"
        assert config.global_config.version == "2.0"
        assert config.global_config.defaults.serialization == "msgpack"
        assert config.global_config.defaults.backpressure.queue_size == 500
        assert len(config.workers) == 2
        assert len(config.components_by_type.get("algorithm", [])) == 2
        assert len(config.components_by_type.get("data_provider", [])) == 1
        assert len(config.connections) == 1

    def test_config_roundtrip_helpers(self, loader: ConfigLoader) -> None:
        """Test that helper methods work correctly after loading."""
        config = loader.load(FIXTURES_DIR / "valid_simple.yaml")

        all_instances = config.get_all_component_instances()
        assert "counter_1" in all_instances
        assert "printer_1" in all_instances

        assert config.get_component_type("counter_1") == "data_provider"
        assert config.get_component_type("printer_1") == "algorithm"

    def test_config_exports_from_main_module(self) -> None:
        """Test that config classes are exported from main vectis module."""
        import vectis

        assert hasattr(vectis, "ConfigLoader")
        assert hasattr(vectis, "PipelineConfig")
        assert hasattr(vectis, "GlobalConfig")
        assert hasattr(vectis, "ConnectionConfig")
        assert hasattr(vectis, "BackpressureConfig")
        assert hasattr(vectis, "DefaultsConfig")
        assert hasattr(vectis, "WorkerConfig")
        assert hasattr(vectis, "ComponentInstanceConfig")
        assert hasattr(vectis, "TransportConfig")
