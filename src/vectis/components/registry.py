"""Vectis component registry system.

This module provides the registry infrastructure for registering and
retrieving component types and component classes. The registries are
singletons that maintain global state for component discovery.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, ClassVar

from vectis.exceptions import ComponentNotFoundError

if TYPE_CHECKING:
    from vectis.components.base import Component


class ComponentTypeRegistry:
    """Registry for component types (e.g., 'algorithm', 'data_provider').

    This is a singleton that maintains a mapping from type names to
    base classes. It's used to create decorators for registering
    concrete component implementations.

    The registry comes pre-populated with built-in types ('algorithm',
    'data_provider') when the types module is imported.

    Example:
        >>> registry = ComponentTypeRegistry()
        >>> registry.register_type("algorithm", Algorithm)
        >>> decorator = registry.create_decorator("algorithm")
        >>> @decorator("my_algo")
        ... class MyAlgo(Algorithm[MyConfig]):
        ...     pass
    """

    _instance: ClassVar[ComponentTypeRegistry | None] = None
    _types: dict[str, type[Any]]

    def __new__(cls) -> ComponentTypeRegistry:
        """Create or return the singleton instance."""
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._types = {}
            cls._instance = instance
        return cls._instance

    def register_type(self, name: str, base_class: type[Any]) -> None:
        """Register a component type with its base class.

        Args:
            name: The type name (e.g., 'algorithm', 'data_provider').
            base_class: The base class for this component type.

        Raises:
            ValueError: If the type name is already registered.
        """
        if name in self._types:
            raise ValueError(f"Component type '{name}' is already registered")
        self._types[name] = base_class

    def get_type(self, name: str) -> type[Any]:
        """Get the base class for a registered component type.

        Args:
            name: The type name to look up.

        Returns:
            The base class for the specified type.

        Raises:
            KeyError: If the type name is not registered.
        """
        if name not in self._types:
            raise KeyError(f"Component type '{name}' is not registered")
        return self._types[name]

    def create_decorator(
        self, type_name: str
    ) -> Callable[[str], Callable[[type[Any]], type[Any]]]:
        """Create a decorator for registering components of a specific type.

        Args:
            type_name: The component type (e.g., 'algorithm').

        Returns:
            A decorator factory that takes a component name and returns
            a decorator that registers the class.

        Raises:
            KeyError: If the type name is not registered.

        Example:
            >>> decorator = registry.create_decorator("algorithm")
            >>> @decorator("my_algo")
            ... class MyAlgo(Algorithm[MyConfig]):
            ...     pass
        """
        if type_name not in self._types:
            raise KeyError(f"Component type '{type_name}' is not registered")

        base_class = self._types[type_name]
        component_registry = ComponentRegistry()

        def decorator_factory(name: str) -> Callable[[type[Any]], type[Any]]:
            def decorator(cls: type[Any]) -> type[Any]:
                if not issubclass(cls, base_class):
                    raise TypeError(
                        f"Class '{cls.__name__}' must be a subclass of "
                        f"'{base_class.__name__}' to be registered as '{type_name}'"
                    )
                component_registry.register_component(name, cls, type_name)
                return cls

            return decorator

        return decorator_factory

    @property
    def types(self) -> dict[str, type[Any]]:
        """Return a copy of the registered types mapping."""
        return self._types.copy()

    def clear(self) -> None:
        """Clear all registered types. Primarily for testing."""
        self._types.clear()


class ComponentRegistry:
    """Registry for concrete component implementations.

    This is a singleton that maintains a mapping from component names
    to their classes and types. Components are registered via decorators
    created by ComponentTypeRegistry.

    Example:
        >>> registry = ComponentRegistry()
        >>> cls = registry.get_component("my_algo")
        >>> instance = ComponentFactory().create_component("my_algo", "instance1", {})
    """

    _instance: ClassVar[ComponentRegistry | None] = None
    _components: dict[str, tuple[type[Any], str]]

    def __new__(cls) -> ComponentRegistry:
        """Create or return the singleton instance."""
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._components = {}
            cls._instance = instance
        return cls._instance

    def register_component(
        self, name: str, component_class: type[Any], component_type: str
    ) -> None:
        """Register a component class with its name and type.

        Args:
            name: The unique name for this component.
            component_class: The component class to register.
            component_type: The type of component (e.g., 'algorithm').

        Raises:
            ValueError: If a component with this name is already registered.
        """
        if name in self._components:
            existing_cls, _ = self._components[name]
            raise ValueError(
                f"Component '{name}' is already registered as {existing_cls.__name__}"
            )
        self._components[name] = (component_class, component_type)

    def get_component(self, name: str) -> type[Any]:
        """Get a registered component class by name.

        Args:
            name: The component name to look up.

        Returns:
            The component class.

        Raises:
            ComponentNotFoundError: If no component is registered with this name.
        """
        if name not in self._components:
            raise ComponentNotFoundError(name)
        return self._components[name][0]

    def get_component_type(self, name: str) -> str:
        """Get the type of a registered component.

        Args:
            name: The component name to look up.

        Returns:
            The component type (e.g., 'algorithm', 'data_provider').

        Raises:
            ComponentNotFoundError: If no component is registered with this name.
        """
        if name not in self._components:
            raise ComponentNotFoundError(name)
        return self._components[name][1]

    @property
    def components(self) -> dict[str, type[Any]]:
        """Return a mapping of component names to classes."""
        return {name: cls for name, (cls, _) in self._components.items()}

    def clear(self) -> None:
        """Clear all registered components. Primarily for testing."""
        self._components.clear()


# Module-level singleton instances for convenience
_component_type_registry = ComponentTypeRegistry()
_component_registry = ComponentRegistry()


def get_component_type_registry() -> ComponentTypeRegistry:
    """Get the global ComponentTypeRegistry instance."""
    return _component_type_registry


def get_component_registry() -> ComponentRegistry:
    """Get the global ComponentRegistry instance."""
    return _component_registry
