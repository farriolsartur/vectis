"""Vectis component base class.

This module provides the abstract base class for all components in the
Vectis pipeline system. Components are parameterized by a configuration
type that defines their runtime settings.
"""

from __future__ import annotations

from abc import ABC
from typing import Any, Generic, TypeVar, get_args, get_origin

from pydantic import BaseModel


class EmptyConfig(BaseModel):
    """Default empty configuration for components that don't need config."""

    pass


ConfigT = TypeVar("ConfigT", bound=BaseModel)


class Component(ABC, Generic[ConfigT]):
    """Abstract base class for all pipeline components.

    Components are the building blocks of Vectis pipelines. Each component
    has a name and a typed configuration. Components support lifecycle hooks
    for initialization and cleanup.

    The configuration type is extracted from the generic parameter at class
    definition time. If no configuration is specified, EmptyConfig is used.

    Attributes:
        name: Unique identifier for this component instance.
        config: The component's configuration (validated Pydantic model).

    Example:
        >>> class MyConfig(BaseModel):
        ...     threshold: float = 0.5
        ...
        >>> class MyAlgorithm(Algorithm[MyConfig]):
        ...     async def on_received_data(self, message):
        ...         if message.payload["value"] > self.config.threshold:
        ...             await self.process(message)

    Lifecycle Hooks:
        - on_start(): Called after wiring, before pipeline starts processing.
                      Use for resource initialization (DB connections, files).
        - on_stop(): Called during graceful shutdown after processing ends.
                     Use for resource cleanup.
    """

    name: str
    config: ConfigT

    def __init__(self, name: str, config: ConfigT) -> None:
        """Initialize the component.

        Args:
            name: Unique identifier for this component instance.
            config: The validated configuration for this component.
        """
        self.name = name
        self.config = config

    @classmethod
    def get_config_class(cls) -> type[BaseModel]:
        """Extract the configuration class from the generic type parameter.

        This method inspects the class's generic bases to find the ConfigT
        type argument. If the class doesn't specify a configuration type,
        EmptyConfig is returned.

        Returns:
            The Pydantic BaseModel subclass used for configuration.

        Example:
            >>> class MyAlgo(Algorithm[MyConfig]):
            ...     pass
            >>> MyAlgo.get_config_class()
            <class 'MyConfig'>
        """
        # Walk through the method resolution order to find generic bases
        for base in cls.__mro__:
            # Check if this class has __orig_bases__ (parameterized generic)
            orig_bases = getattr(base, "__orig_bases__", ())
            for orig_base in orig_bases:
                origin = get_origin(orig_base)
                # Look for Component or its subclasses
                if origin is not None and _is_component_origin(origin):
                    args = get_args(orig_base)
                    if args:
                        config_class = args[0]
                        # Handle TypeVar (unspecified) vs actual type
                        if isinstance(config_class, TypeVar):
                            continue
                        if isinstance(config_class, type) and issubclass(
                            config_class, BaseModel
                        ):
                            return config_class

        # No config type found, return default
        return EmptyConfig

    async def on_start(self) -> None:
        """Lifecycle hook called before the pipeline starts processing.

        Override this method to perform initialization that requires
        async operations or resources that should be set up after the
        component is wired but before data flows.

        Examples:
            - Opening database connections
            - Loading ML models
            - Connecting to external services
            - Initializing file handles

        The default implementation is a no-op.
        """
        pass

    async def on_stop(self) -> None:
        """Lifecycle hook called during graceful shutdown.

        Override this method to perform cleanup after the component
        has finished processing. This is called after END_OF_STREAM
        has propagated through the pipeline.

        Examples:
            - Closing database connections
            - Flushing buffers
            - Releasing external resources
            - Saving state

        The default implementation is a no-op.
        """
        pass


def _is_component_origin(origin: Any) -> bool:
    """Check if an origin type is Component or a Component subclass.

    This handles the case where the origin might be a different
    parameterized generic but still inherits from Component.
    """
    try:
        return origin is Component or (
            isinstance(origin, type) and issubclass(origin, Component)
        )
    except TypeError:
        # issubclass raises TypeError for some special forms
        return False
