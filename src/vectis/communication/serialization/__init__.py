"""Vectis serialization module."""

from vectis.communication.protocols import Serializer
from vectis.communication.serialization.json_serializer import JSONSerializer
from vectis.communication.serialization.msgpack_serializer import (
    MessagePackSerializer,
)

__all__ = [
    "Serializer",
    "JSONSerializer",
    "MessagePackSerializer",
    "get_serializer",
]


def get_serializer(name: str) -> Serializer:
    """Get a serializer by name.

    Args:
        name: Serializer name ('json' or 'msgpack').

    Returns:
        Serializer instance.

    Raises:
        ValueError: If name is not recognized.

    Example:
        >>> serializer = get_serializer("json")
        >>> isinstance(serializer, JSONSerializer)
        True
    """
    serializers: dict[str, type[Serializer]] = {
        "json": JSONSerializer,
        "msgpack": MessagePackSerializer,
    }

    if name not in serializers:
        raise ValueError(
            f"Unknown serializer '{name}'. Available: {list(serializers.keys())}"
        )

    return serializers[name]()
