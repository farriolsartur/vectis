"""Vectis configuration loader for YAML pipeline definitions.

This module provides the ConfigLoader class that:
1. Reads and parses YAML configuration files
2. Transforms raw YAML into validated PipelineConfig
3. Validates component references against the registry
4. Validates pipeline topology for correctness
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from vectis.components.registry import (
    ComponentRegistry,
    get_component_registry,
    get_component_type_registry,
)
from vectis.exceptions import ComponentNotFoundError, PipelineConfigError

from .models import (
    ComponentInstanceConfig,
    ConnectionConfig,
    GlobalConfig,
    PipelineConfig,
    WorkerConfig,
)


class ConfigLoader:
    """Loader for YAML pipeline configurations.

    The ConfigLoader reads YAML files, parses them into Pydantic models,
    and validates the configuration against the component registry and
    topology rules.

    Example:
        >>> loader = ConfigLoader()
        >>> config = loader.load("pipeline.yaml")
        >>> errors = loader.validate(config)
        >>> if errors:
        ...     for error in errors:
        ...         print(error)
    """

    # Reserved section names that are not component types
    RESERVED_SECTIONS = frozenset({
        "global",
        "workers",
        "connections",
    })

    def __init__(self, registry: ComponentRegistry | None = None) -> None:
        """Initialize the loader.

        Args:
            registry: Optional ComponentRegistry for validation.
                      Uses global registry if not provided.
        """
        self._registry = registry or get_component_registry()

    def load(self, yaml_path: str | Path) -> PipelineConfig:
        """Load and parse a YAML configuration file.

        This method:
        1. Reads the YAML file
        2. Parses sections into appropriate config models
        3. Extracts component instances from type-specific sections
        4. Returns a validated PipelineConfig

        Args:
            yaml_path: Path to the YAML configuration file.

        Returns:
            Validated PipelineConfig instance.

        Raises:
            PipelineConfigError: If file cannot be read, parsed, or validated.
        """
        path = Path(yaml_path)

        # Read YAML file
        try:
            with path.open("r", encoding="utf-8") as f:
                raw_config = yaml.safe_load(f)
        except FileNotFoundError as e:
            raise PipelineConfigError(f"Configuration file not found: {path}") from e
        except yaml.YAMLError as e:
            raise PipelineConfigError(f"Invalid YAML in {path}: {e}") from e

        if not isinstance(raw_config, dict):
            raise PipelineConfigError(
                f"Configuration must be a YAML mapping, got {type(raw_config).__name__}"
            )

        return self._parse_raw_config(raw_config, str(path))

    def load_from_string(self, yaml_content: str) -> PipelineConfig:
        """Load configuration from a YAML string.

        Useful for testing and programmatic configuration.

        Args:
            yaml_content: YAML configuration as a string.

        Returns:
            Validated PipelineConfig instance.

        Raises:
            PipelineConfigError: If content cannot be parsed or validated.
        """
        try:
            raw_config = yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            raise PipelineConfigError(f"Invalid YAML: {e}") from e

        if not isinstance(raw_config, dict):
            raise PipelineConfigError(
                f"Configuration must be a YAML mapping, got {type(raw_config).__name__}"
            )

        return self._parse_raw_config(raw_config, "<string>")

    def _parse_raw_config(
        self, raw_config: dict[str, Any], source: str
    ) -> PipelineConfig:
        """Parse raw YAML dict into PipelineConfig.

        Args:
            raw_config: Parsed YAML dictionary.
            source: Source file path or identifier for error messages.

        Returns:
            Validated PipelineConfig.

        Raises:
            PipelineConfigError: On validation errors.
        """
        try:
            # Parse global config (required)
            global_section = raw_config.get("global")
            if global_section is None:
                raise PipelineConfigError(
                    f"Configuration in {source} is missing required 'global' section"
                )
            if not isinstance(global_section, dict):
                raise PipelineConfigError("'global' section must be a mapping")
            global_config = GlobalConfig.model_validate(global_section)

            # Parse workers (optional)
            workers_section = raw_config.get("workers", [])
            if not isinstance(workers_section, list):
                raise PipelineConfigError("'workers' section must be a list")
            workers = [WorkerConfig.model_validate(w) for w in workers_section]

            # Parse connections (optional but usually needed)
            connections_section = raw_config.get("connections", [])
            if not isinstance(connections_section, list):
                raise PipelineConfigError("'connections' section must be a list")
            connections = [
                ConnectionConfig.model_validate(c) for c in connections_section
            ]

            # Extract component instances from remaining sections
            components_by_type = self._extract_components(raw_config)

            return PipelineConfig(
                global_config=global_config,
                workers=workers,
                connections=connections,
                components_by_type=components_by_type,
            )

        except ValidationError as e:
            raise PipelineConfigError(
                f"Configuration validation failed for {source}: {e}"
            ) from e

    def _extract_components(
        self, raw_config: dict[str, Any]
    ) -> dict[str, list[ComponentInstanceConfig]]:
        """Extract component instances from type-specific YAML sections.

        Any top-level key that is not in RESERVED_SECTIONS is treated as
        a component type. For example:

            algorithms:
              - name: my_algo
                config: {threshold: 0.5}

            data_providers:
              - name: my_provider
                config: {count: 100}

        Args:
            raw_config: Full parsed YAML dictionary.

        Returns:
            Mapping from component type to list of instance configs.
        """
        components_by_type: dict[str, list[ComponentInstanceConfig]] = {}

        for section_name, section_content in raw_config.items():
            if section_name in self.RESERVED_SECTIONS:
                continue

            if not isinstance(section_content, list):
                raise PipelineConfigError(
                    f"Component section '{section_name}' must be a list of instances"
                )

            instances: list[ComponentInstanceConfig] = []
            for item in section_content:
                if not isinstance(item, dict):
                    raise PipelineConfigError(
                        f"Component instance in '{section_name}' must be a mapping"
                    )
                instances.append(ComponentInstanceConfig.model_validate(item))

            # Convert section name to component type (e.g., algorithms -> algorithm)
            component_type = self._normalize_type_name(section_name)
            components_by_type[component_type] = instances

        return components_by_type

    def _normalize_type_name(self, section_name: str) -> str:
        """Normalize section name to component type.

        Handles pluralization: 'algorithms' -> 'algorithm'
        Handles snake_case: 'data_providers' -> 'data_provider'

        Args:
            section_name: The YAML section name.

        Returns:
            Normalized component type name.
        """
        # Remove trailing 's' for common plural forms
        if section_name.endswith("s") and not section_name.endswith("ss"):
            return section_name[:-1]
        return section_name

    def validate(self, config: PipelineConfig) -> list[str]:
        """Validate configuration against topology rules.

        This method performs semantic validation:
        1. Checks for duplicate instance names
        2. Verifies all connection sources and targets reference valid instances
        3. Checks for self-loops
        4. Validates worker references if workers are defined

        Args:
            config: The PipelineConfig to validate.

        Returns:
            List of validation error messages. Empty list means valid.
        """
        errors: list[str] = []

        # Collect all instance names for reference checking
        all_instances = config.get_all_component_instances()
        instance_names = set(all_instances.keys())

        # Check for duplicate instance names
        errors.extend(self._check_duplicate_instances(config))

        # Validate connections
        errors.extend(self._validate_connections(config, instance_names))

        # Validate worker references
        errors.extend(self._validate_workers(config))

        return errors

    def _check_duplicate_instances(self, config: PipelineConfig) -> list[str]:
        """Check for duplicate instance names across all component types.

        Args:
            config: The pipeline configuration.

        Returns:
            List of validation errors.
        """
        errors: list[str] = []
        seen_names: set[str] = set()

        for comp_type, instances in config.components_by_type.items():
            for instance in instances:
                if instance.name in seen_names:
                    errors.append(
                        f"Duplicate component instance name: '{instance.name}'"
                    )
                seen_names.add(instance.name)

        return errors

    def _validate_connections(
        self, config: PipelineConfig, instance_names: set[str]
    ) -> list[str]:
        """Validate connection source and target references.

        Args:
            config: The pipeline configuration.
            instance_names: Set of all valid instance names.

        Returns:
            List of validation errors.
        """
        errors: list[str] = []
        defaults = config.global_config.defaults

        # Track distribution settings per source to detect conflicts
        # source -> (connection_index, effective_distribution, effective_strategy)
        source_settings: dict[str, tuple[int, str, str]] = {}

        # Track fixed ports for collision detection
        # port -> (connection_index, source, target)
        fixed_ports: dict[int, tuple[int, str, str]] = {}

        for i, conn in enumerate(config.connections):
            # Validate source
            if conn.source not in instance_names:
                errors.append(
                    f"Connection {i}: source '{conn.source}' not found. "
                    f"Available: {sorted(instance_names)}"
                )

            # Validate targets
            for target in conn.targets:
                if target not in instance_names:
                    errors.append(
                        f"Connection {i}: target '{target}' not found. "
                        f"Available: {sorted(instance_names)}"
                    )

            # Check for self-loops
            if conn.source in conn.targets:
                errors.append(
                    f"Connection {i}: component '{conn.source}' cannot target itself"
                )

            # Check for conflicting distribution/strategy on same source
            effective_dist = (conn.distribution or defaults.distribution).value
            effective_strat = (conn.strategy or defaults.strategy).value

            if conn.source in source_settings:
                prev_idx, prev_dist, prev_strat = source_settings[conn.source]
                if effective_dist != prev_dist or effective_strat != prev_strat:
                    errors.append(
                        f"Connection {i}: source '{conn.source}' has conflicting "
                        f"distribution settings with connection {prev_idx}. "
                        f"Connection {prev_idx}: distribution={prev_dist}, strategy={prev_strat}. "
                        f"Connection {i}: distribution={effective_dist}, strategy={effective_strat}. "
                        f"A source component can only have one distribution mode across all its connections."
                    )
            else:
                source_settings[conn.source] = (i, effective_dist, effective_strat)

            # Validate ports field
            if conn.ports:
                for target, port in conn.ports.items():
                    # Check: port key must be valid target
                    if target not in conn.targets:
                        errors.append(
                            f"Connection {i}: port specified for '{target}' "
                            f"but '{target}' is not in targets list {conn.targets}"
                        )
                    # Check: no duplicate fixed ports
                    if port in fixed_ports:
                        prev_idx, prev_source, prev_target = fixed_ports[port]
                        errors.append(
                            f"Connection {i}: port {port} for "
                            f"'{conn.source}->{target}' conflicts with "
                            f"connection {prev_idx}: '{prev_source}->{prev_target}'"
                        )
                    else:
                        fixed_ports[port] = (i, conn.source, target)

        return errors

    def _validate_workers(self, config: PipelineConfig) -> list[str]:
        """Validate worker configuration and references.

        Args:
            config: The pipeline configuration.

        Returns:
            List of validation errors.
        """
        errors: list[str] = []

        if not config.workers:
            # No workers defined, skip validation
            return errors

        worker_names = {w.name for w in config.workers}

        # Check for duplicate worker names
        seen_workers: set[str] = set()
        for worker in config.workers:
            if worker.name in seen_workers:
                errors.append(f"Duplicate worker name: '{worker.name}'")
            seen_workers.add(worker.name)

        # Check that worker references in components are valid
        for comp_type, instances in config.components_by_type.items():
            for instance in instances:
                if instance.worker and instance.worker not in worker_names:
                    errors.append(
                        f"Component '{instance.name}' references unknown "
                        f"worker '{instance.worker}'. Available: {sorted(worker_names)}"
                    )

        return errors

    def validate_with_registry(self, config: PipelineConfig) -> list[str]:
        """Extended validation that checks against component registry.

        This validates that:
        1. Component type sections correspond to registered component types
        2. Component instance 'type' fields reference registered components
        3. Joiner components have valid join configuration

        Args:
            config: The pipeline configuration.

        Returns:
            List of validation errors.
        """
        errors = self.validate(config)

        # Validate component type sections
        type_registry = get_component_type_registry()
        registered_types = set(type_registry.types.keys())

        for comp_type in config.components_by_type.keys():
            if comp_type not in registered_types:
                errors.append(
                    f"Unknown component type '{comp_type}'. "
                    f"Registered types: {sorted(registered_types)}"
                )

        # Validate component instance 'type' fields against ComponentRegistry
        for section_type, instances in config.components_by_type.items():
            for instance in instances:
                try:
                    self._registry.get_component(instance.type)
                except ComponentNotFoundError:
                    errors.append(
                        f"Component '{instance.name}' references unknown type "
                        f"'{instance.type}'. Check component registration."
                    )

        # Validate joiner configurations
        errors.extend(self._validate_joiners(config))

        return errors

    def _validate_joiners(self, config: PipelineConfig) -> list[str]:
        """Validate joiner configurations.

        Args:
            config: The pipeline configuration.

        Returns:
            List of validation errors.
        """
        errors: list[str] = []

        # Build map of target -> sources from connections
        target_sources: dict[str, set[str]] = {}
        for conn in config.connections:
            for target in conn.targets:
                target_sources.setdefault(target, set()).add(conn.source)

        # Validate joiner-type components
        joiner_instances = config.components_by_type.get("joiner", [])
        for instance in joiner_instances:
            if instance.join is None:
                errors.append(
                    f"Joiner '{instance.name}' is missing required 'join' configuration"
                )
                continue

            # Validate join config structure
            try:
                from vectis.components.joining.config import JoinConfig

                join_config = JoinConfig.model_validate(instance.join)
            except Exception as e:
                errors.append(
                    f"Invalid join configuration for '{instance.name}': {e}"
                )
                continue

            # Validate that declared sources match actual connections
            expected_sources = set(join_config.sources)
            actual_sources = target_sources.get(instance.name, set())

            if expected_sources != actual_sources:
                missing = expected_sources - actual_sources
                extra = actual_sources - expected_sources

                if missing:
                    errors.append(
                        f"Joiner '{instance.name}': missing connections from "
                        f"declared sources: {sorted(missing)}"
                    )
                if extra:
                    errors.append(
                        f"Joiner '{instance.name}': has connections from "
                        f"undeclared sources: {sorted(extra)}"
                    )

        return errors
