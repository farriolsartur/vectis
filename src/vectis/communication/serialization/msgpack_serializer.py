"""MessagePack serializer for Vectis messages."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from vectis.communication.protocols import Serializer
from vectis.messages import Message

if TYPE_CHECKING:
    import msgpack as msgpack_module


class MessagePackSerializer(Serializer):
    """MessagePack-based message serializer.

    Binary format that is more compact and faster than JSON.
    Requires the optional 'msgpack' dependency.

    Note:
        Install with: pip install vectis[msgpack]

    Example:
        >>> serializer = MessagePackSerializer()
        >>> msg = Message.data(payload={"x": 42}, source_component="test")
        >>> data = serializer.serialize(msg)
        >>> restored = serializer.deserialize(data)
        >>> restored.payload == {"x": 42}
        True
    """

    _msgpack: msgpack_module

    def __init__(self) -> None:
        """Initialize the MessagePack serializer.

        Raises:
            ImportError: If msgpack package is not installed.
        """
        try:
            # Lazy import to prevent crashes
            import msgpack

            self._msgpack = msgpack
        except ImportError as e:
            raise ImportError(
                "msgpack package is required for MessagePackSerializer. "
                "Install with: pip install vectis[msgpack]"
            ) from e

    def serialize(self, message: Message[Any]) -> bytes:
        """Serialize a message to MessagePack bytes.

        Args:
            message: The message to serialize.

        Returns:
            MessagePack-encoded bytes.
        """
        data = message.model_dump(mode="json")
        return self._msgpack.packb(data, use_bin_type=True)

    def deserialize(self, data: bytes) -> Message[Any]:
        """Deserialize MessagePack bytes back to a message.

        Args:
            data: MessagePack-encoded bytes.

        Returns:
            Reconstructed Message object.

        Raises:
            msgpack.UnpackException: If data is not valid MessagePack.
            pydantic.ValidationError: If data doesn't match Message schema.
        """
        parsed = self._msgpack.unpackb(data, raw=False)
        return Message.model_validate(parsed)
