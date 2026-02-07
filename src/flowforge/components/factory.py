"""FlowForge component factory for creating component instances.

This module provides the ComponentFactory class that creates component
instances from registry entries and configuration dictionaries.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from flowforge.components.base import Component
from flowforge.components.registry import (
    ComponentRegistry,
    get_component_registry,
)
from flowforge.exceptions import ComponentNotFoundError, PipelineConfigError

if TYPE_CHECKING:
    from flowforge.components.joining.config import JoinConfig


class ComponentFactory:
    """Factory for creating component instances from configuration.

    The ComponentFactory looks up component classes in the registry,
    extracts their configuration class, validates the configuration,
    and creates instances.

    Example:
        >>> factory = ComponentFactory()
        >>> provider = factory.create_component(
        ...     component_name="counter_provider",
        ...     instance_name="my_counter",
        ...     config_dict={"count": 100, "interval": 0.1}
        ... )
    """

    def __init__(self, registry: ComponentRegistry | None = None) -> None:
        """Initialize the factory.

        Args:
            registry: Optional ComponentRegistry to use. If not provided,
                      the global registry is used.
        """
        self._registry = registry or get_component_registry()

    def create_component(
        self,
        component_name: str,
        instance_name: str,
        config_dict: dict[str, Any] | None = None,
        join_config: JoinConfig | None = None,
    ) -> Component[Any]:
        """Create a component instance from registry and configuration.

        This method:
        1. Looks up the component class in the registry
        2. Extracts the configuration class via get_config_class()
        3. Validates and instantiates the configuration
        4. Creates and returns the component instance

        Args:
            component_name: The registered name of the component class
                           (as used in @algorithm or @data_provider decorator).
            instance_name: The unique name for this component instance
                          (used as component.name).
            config_dict: Configuration values to pass to the component.
                        If None, an empty dict is used.
            join_config: Optional join configuration for Joiner components.

        Returns:
            A new component instance with validated configuration.

        Raises:
            ComponentNotFoundError: If component_name is not in the registry.
            PipelineConfigError: If configuration validation fails.

        Example:
            >>> factory = ComponentFactory()
            >>> algo = factory.create_component(
            ...     "printer_algorithm",
            ...     "printer_1",
            ...     {"prefix": "[OUTPUT]"}
            ... )
        """
        config_dict = config_dict or {}

        # Look up component class in registry
        try:
            component_class = self._registry.get_component(component_name)
        except ComponentNotFoundError:
            raise

        # Extract config class from generic parameter
        config_class = component_class.get_config_class()

        # Validate and instantiate configuration
        try:
            config = config_class.model_validate(config_dict)
        except ValidationError as e:
            raise PipelineConfigError(
                f"Invalid configuration for component '{instance_name}' "
                f"(type: {component_name}): {e}"
            ) from e

        # Check if this is a Joiner subclass
        from flowforge.components.joining.joiner import Joiner

        if issubclass(component_class, Joiner):
            if join_config is None:
                raise PipelineConfigError(
                    f"Joiner '{instance_name}' requires 'join' configuration"
                )
            return component_class(
                name=instance_name,
                config=config,
                join_config=join_config,
            )

        # Create and return component instance
        return component_class(name=instance_name, config=config)

    def get_component_class(self, component_name: str) -> type[Component[Any]]:
        """Get a component class from the registry without instantiating.

        Args:
            component_name: The registered name of the component.

        Returns:
            The component class.

        Raises:
            ComponentNotFoundError: If component_name is not in the registry.
        """
        return self._registry.get_component(component_name)
