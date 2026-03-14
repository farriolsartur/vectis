"""JSON serializer for Vectis messages."""

from __future__ import annotations

import json
from typing import Any

from vectis.communication.protocols import Serializer
from vectis.messages import Message


class JSONSerializer(Serializer):
    """JSON-based message serializer.

    Converts Message objects to JSON bytes and back. Human-readable
    but slower than binary formats like MessagePack.

    Attributes:
        _indent: Optional indentation for pretty printing (None for compact).
        _ensure_ascii: If True, escape non-ASCII characters.

    Example:
        >>> serializer = JSONSerializer()
        >>> msg = Message.data(payload={"x": 42}, source_component="test")
        >>> data = serializer.serialize(msg)
        >>> restored = serializer.deserialize(data)
        >>> restored.payload == {"x": 42}
        True
    """

    def __init__(
        self,
        indent: int | None = None,
        ensure_ascii: bool = False,
    ) -> None:
        """Initialize the JSON serializer.

        Args:
            indent: JSON indentation level (None for compact).
            ensure_ascii: Whether to escape non-ASCII characters.
        """
        self._indent = indent
        self._ensure_ascii = ensure_ascii

    def serialize(self, message: Message[Any]) -> bytes:
        """Serialize a message to JSON bytes.

        Uses Pydantic's model_dump with mode='json' to ensure
        UUID and datetime are converted to strings.

        Args:
            message: The message to serialize.

        Returns:
            JSON-encoded bytes.
        """
        data = message.model_dump(mode="json")
        json_str = json.dumps(
            data,
            indent=self._indent,
            ensure_ascii=self._ensure_ascii,
        )
        return json_str.encode("utf-8")

    def deserialize(self, data: bytes) -> Message[Any]:
        """Deserialize JSON bytes back to a message.

        Args:
            data: JSON-encoded bytes.

        Returns:
            Reconstructed Message object.

        Raises:
            json.JSONDecodeError: If data is not valid JSON.
            pydantic.ValidationError: If data doesn't match Message schema.
        """
        json_str = data.decode("utf-8")
        parsed = json.loads(json_str)
        return Message.model_validate(parsed)
