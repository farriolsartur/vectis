"""FlowForge convenience decorators for component registration.

This module provides decorator functions for registering custom
component classes with the global registry. These decorators are
syntactic sugar over ComponentTypeRegistry.create_decorator().
"""

from __future__ import annotations

from typing import Callable, TypeVar

from flowforge.components.registry import get_component_type_registry

# Import types module to ensure built-in types are registered
import flowforge.components.types  # noqa: F401

T = TypeVar("T", bound=type)


def algorithm(name: str) -> Callable[[T], T]:
    """Decorator for registering a class as an algorithm component.

    The decorated class must be a subclass of Algorithm.

    Args:
        name: Unique name for this algorithm in the registry.

    Returns:
        A decorator that registers the class as an algorithm.

    Raises:
        TypeError: If the decorated class is not a subclass of Algorithm.
        ValueError: If an algorithm with this name is already registered.

    Example:
        >>> @algorithm("my_processor")
        ... class MyProcessor(Algorithm[MyConfig]):
        ...     async def on_received_data(self, message):
        ...         process(message.payload)
    """
    registry = get_component_type_registry()
    return registry.create_decorator("algorithm")(name)


def data_provider(name: str) -> Callable[[T], T]:
    """Decorator for registering a class as a data provider component.

    The decorated class must be a subclass of DataProvider.

    Args:
        name: Unique name for this data provider in the registry.

    Returns:
        A decorator that registers the class as a data provider.

    Raises:
        TypeError: If the decorated class is not a subclass of DataProvider.
        ValueError: If a data provider with this name is already registered.

    Example:
        >>> @data_provider("my_source")
        ... class MySource(DataProvider[MyConfig]):
        ...     async def run(self):
        ...         for item in self.generate_items():
        ...             await self.send_data(item)
        ...         await self.send_end_of_stream()
    """
    registry = get_component_type_registry()
    return registry.create_decorator("data_provider")(name)


def joiner(name: str) -> Callable[[T], T]:
    """Decorator for registering a class as a joiner component.

    The decorated class must be a subclass of Joiner.

    Args:
        name: Unique name for this joiner in the registry.

    Returns:
        A decorator that registers the class as a joiner.

    Raises:
        TypeError: If the decorated class is not a subclass of Joiner.
        ValueError: If a joiner with this name is already registered.

    Example:
        >>> @joiner("order_enricher")
        ... class OrderEnricher(Joiner[EnricherConfig]):
        ...     async def on_joined(self, key, messages):
        ...         order = messages["orders"][0].payload
        ...         customer = messages["customers"][0].payload
        ...         await self.send_data({**order, "customer": customer})
    """
    registry = get_component_type_registry()
    return registry.create_decorator("joiner")(name)
