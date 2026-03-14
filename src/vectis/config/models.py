"""Vectis configuration models for YAML-based pipeline definition.

This module provides Pydantic models for parsing and validating pipeline
configurations from YAML files. The configuration hierarchy is:

    PipelineConfig
    ├── GlobalConfig
    │   ├── DefaultsConfig
    │   │   └── BackpressureConfig
    │   └── TransportConfig (optional)
    ├── WorkerConfig[] (optional)
    ├── ConnectionConfig[]
    └── components_by_type: Dict[str, ComponentInstanceConfig[]]
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from vectis.communication.enums import (
    BackpressureMode,
    CompetingStrategy,
    DistributionMode,
    StartupSyncStrategy,
)


class BackpressureConfig(BaseModel):
    """Configuration for backpressure handling.

    Attributes:
        mode: How to handle full queues (block or drop).
        queue_size: Maximum queue size before backpressure triggers.
            Used by INPROCESS and MULTIPROCESS channels.

    Note:
        For DISTRIBUTED (ZMQ) channels, backpressure is controlled by
        ``transport.config.high_water_mark`` instead of ``queue_size``.
        ZMQ uses socket-level high water marks rather than queue depths.
    """

    model_config = ConfigDict(extra="forbid")

    mode: BackpressureMode = BackpressureMode.BLOCK
    queue_size: int = Field(default=1000, ge=1)

    @field_validator("mode", mode="before")
    @classmethod
    def parse_mode(cls, v: Any) -> BackpressureMode:
        """Allow string input for mode."""
        if isinstance(v, str):
            return BackpressureMode(v.lower())
        return v


class DefaultsConfig(BaseModel):
    """Default configuration values applied to all connections.

    These defaults can be overridden per-connection in ConnectionConfig.

    Attributes:
        serialization: Default serializer (json or msgpack).
        distribution: Default distribution mode (fan_out or competing).
        strategy: Default competing strategy when distribution is competing.
        backpressure: Default backpressure configuration.
    """

    model_config = ConfigDict(extra="forbid")

    serialization: Literal["json", "msgpack"] = "json"
    distribution: DistributionMode = DistributionMode.FAN_OUT
    strategy: CompetingStrategy = CompetingStrategy.ROUND_ROBIN
    backpressure: BackpressureConfig = Field(default_factory=BackpressureConfig)

    @field_validator("distribution", mode="before")
    @classmethod
    def parse_distribution(cls, v: Any) -> DistributionMode:
        """Allow string input for distribution mode."""
        if isinstance(v, str):
            # Handle both 'fan_out' and 'fan-out' formats
            normalized = v.lower().replace("-", "_")
            return DistributionMode(normalized)
        return v

    @field_validator("strategy", mode="before")
    @classmethod
    def parse_strategy(cls, v: Any) -> CompetingStrategy:
        """Allow string input for strategy."""
        if isinstance(v, str):
            normalized = v.lower().replace("-", "_")
            return CompetingStrategy(normalized)
        return v


class TransportConfig(BaseModel):
    """Configuration for transport layer (Phase 6 feature).

    Attributes:
        type: Transport type identifier.
        config: Transport-specific configuration. Supported keys:
            - ``high_water_mark`` (int): ZMQ socket buffer limit for
              backpressure (default: 1000). Controls how many messages
              can be buffered before ZMQ blocks or drops.
            - ``base_port`` (int): Starting port for ZMQ endpoints.
            - ``port_range`` (int): Port range for endpoint generation.
            - ``protocol`` (str): ZMQ protocol (default: "tcp").
            - ``endpoint_template`` (str): Custom endpoint format string.
    """

    model_config = ConfigDict(extra="forbid")

    type: str
    config: dict[str, Any] = Field(default_factory=dict)


class WorkerConfig(BaseModel):
    """Configuration for a worker process.

    Workers define process boundaries for distributed execution.

    Attributes:
        name: Unique worker identifier.
        host: Host where this worker runs.
        config: Worker-specific configuration.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    host: str = Field(default="localhost")
    config: dict[str, Any] = Field(default_factory=dict)


class ComponentInstanceConfig(BaseModel):
    """Configuration for a component instance.

    Attributes:
        name: Unique instance name (used in connections).
        type: Registered component implementation name (from ComponentRegistry).
        worker: Optional worker assignment for distributed execution.
        config: Component-specific configuration passed to factory.
        join: Optional join configuration for Joiner components.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    type: str = Field(min_length=1)
    worker: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    join: dict[str, Any] | None = Field(
        default=None,
        description="Join configuration for Joiner components",
    )


class ConnectionConfig(BaseModel):
    """Configuration for a connection between components.

    Defines the data flow from a source component to one or more targets.

    Attributes:
        source: Name of the sending component.
        targets: List of receiving component names.
        distribution: How to distribute messages (overrides defaults).
        strategy: Competing strategy (overrides defaults).
        serialization: Serializer to use (overrides defaults).
        backpressure: Backpressure config (overrides defaults).
        ports: Per-target port overrides for distributed transport. Keys must
            be valid target names. Targets without port overrides use hash-based
            port generation.
    """

    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1)
    targets: list[str] = Field(min_length=1)
    distribution: DistributionMode | None = None
    strategy: CompetingStrategy | None = None
    serialization: Literal["json", "msgpack"] | None = None
    backpressure: BackpressureConfig | None = None
    ports: dict[str, int] | None = Field(
        default=None,
        description="Per-target port overrides. Keys must be valid target names.",
    )

    @field_validator("ports", mode="after")
    @classmethod
    def validate_port_values(cls, v: dict[str, int] | None) -> dict[str, int] | None:
        """Validate port values are in valid range."""
        if v is None:
            return None
        for target, port in v.items():
            if not (1 <= port <= 65535):
                raise ValueError(
                    f"Port {port} for target '{target}' must be between 1 and 65535"
                )
        return v

    @field_validator("distribution", mode="before")
    @classmethod
    def parse_distribution(cls, v: Any) -> DistributionMode | None:
        """Allow string input for distribution mode."""
        if v is None:
            return None
        if isinstance(v, str):
            normalized = v.lower().replace("-", "_")
            return DistributionMode(normalized)
        return v

    @field_validator("strategy", mode="before")
    @classmethod
    def parse_strategy(cls, v: Any) -> CompetingStrategy | None:
        """Allow string input for strategy."""
        if v is None:
            return None
        if isinstance(v, str):
            normalized = v.lower().replace("-", "_")
            return CompetingStrategy(normalized)
        return v


class GlobalConfig(BaseModel):
    """Global pipeline configuration.

    Attributes:
        name: Pipeline name for identification.
        version: Configuration version string.
        defaults: Default values for connections.
        transport: Optional transport layer configuration.
        sync_strategy: How to synchronize component startup.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    version: str = Field(default="1.0")
    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    transport: TransportConfig | None = None
    sync_strategy: StartupSyncStrategy = StartupSyncStrategy.RETRY_BACKOFF

    @field_validator("sync_strategy", mode="before")
    @classmethod
    def parse_sync_strategy(cls, v: Any) -> StartupSyncStrategy:
        """Allow string input for sync strategy."""
        if isinstance(v, str):
            normalized = v.lower().replace("-", "_")
            return StartupSyncStrategy(normalized)
        return v


class PipelineConfig(BaseModel):
    """Complete pipeline configuration.

    This is the root model for YAML pipeline definitions. It contains
    all configuration needed to build and run a pipeline.

    Attributes:
        global_config: Global pipeline settings.
        workers: List of worker configurations (empty for single-process).
        connections: List of component connections defining data flow.
        components_by_type: Mapping from component type to instances.
    """

    model_config = ConfigDict(extra="forbid")

    global_config: GlobalConfig
    workers: list[WorkerConfig] = Field(default_factory=list)
    connections: list[ConnectionConfig] = Field(default_factory=list)
    components_by_type: dict[str, list[ComponentInstanceConfig]] = Field(
        default_factory=dict
    )

    def get_all_component_instances(self) -> dict[str, ComponentInstanceConfig]:
        """Return a flat mapping of instance name to config.

        Returns:
            Dictionary mapping instance names to their configurations.
        """
        result: dict[str, ComponentInstanceConfig] = {}
        for instances in self.components_by_type.values():
            for instance in instances:
                result[instance.name] = instance
        return result

    def get_component_type(self, instance_name: str) -> str | None:
        """Get the component type for a given instance name.

        Args:
            instance_name: The name of the component instance.

        Returns:
            The component type name, or None if not found.
        """
        for component_type, instances in self.components_by_type.items():
            for instance in instances:
                if instance.name == instance_name:
                    return component_type
        return None
