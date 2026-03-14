"""Vectis message types for inter-component communication."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Generic, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class MessageType(Enum):
    """Type of message being sent between components."""

    DATA = "data"
    """Normal data payload message."""

    ERROR = "error"
    """Error message indicating a problem occurred."""

    END_OF_STREAM = "end_of_stream"
    """Signal that no more messages will be sent from this source."""


T = TypeVar("T")


class Message(BaseModel, Generic[T]):
    """Generic message container for inter-component communication.

    The Message class is generic (`Message[T]`) but payload type enforcement
    is the receiver's responsibility. After serialization/deserialization
    (required for multiprocess and distributed communication), payloads become
    dictionaries regardless of their original type.

    If a sender uses a Pydantic model as payload, the `payload_type` field
    stores the original type path as a hint. Receivers that require structured
    data should validate/parse the payload dict into their expected type.

    Attributes:
        id: Unique identifier for this message (auto-generated).
        timestamp: When the message was created (UTC).
        message_type: Type of message (DATA, ERROR, END_OF_STREAM).
        payload: The message data (type varies, becomes dict after serialization).
        source_component: Name of the component that created this message.
        payload_type: Optional type path hint for Pydantic payload reconstruction.

    Example:
        >>> msg = Message(
        ...     message_type=MessageType.DATA,
        ...     payload={"value": 42},
        ...     source_component="my_provider"
        ... )
        >>> msg.id  # Auto-generated UUID
        UUID('...')
    """

    model_config = ConfigDict(frozen=True)

    id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    message_type: MessageType
    payload: T = Field(default=None)  # type: ignore[assignment]
    source_component: str
    payload_type: str | None = Field(
        default=None,
        description="Type path (e.g., 'mymodule.MyModel') for Pydantic payload reconstruction",
    )

    @classmethod
    def data(
        cls,
        payload: Any,
        source_component: str,
        payload_type: str | None = None,
    ) -> Message[Any]:
        """Create a DATA message with the given payload.

        Args:
            payload: The data to send.
            source_component: Name of the sending component.
            payload_type: Optional type path for Pydantic models.

        Returns:
            A new Message with message_type=DATA.
        """
        return cls(
            message_type=MessageType.DATA,
            payload=payload,
            source_component=source_component,
            payload_type=payload_type,
        )

    @classmethod
    def error(
        cls,
        error: str | Exception,
        source_component: str,
    ) -> Message[str]:
        """Create an ERROR message.

        Args:
            error: Error message or exception.
            source_component: Name of the sending component.

        Returns:
            A new Message with message_type=ERROR.
        """
        error_str = str(error) if isinstance(error, Exception) else error
        return cls(
            message_type=MessageType.ERROR,
            payload=error_str,
            source_component=source_component,
        )

    @classmethod
    def end_of_stream(cls, source_component: str) -> Message[None]:
        """Create an END_OF_STREAM message.

        Args:
            source_component: Name of the sending component.

        Returns:
            A new Message with message_type=END_OF_STREAM.
        """
        return cls(
            message_type=MessageType.END_OF_STREAM,
            payload=None,
            source_component=source_component,
        )

    @property
    def is_data(self) -> bool:
        """Check if this is a DATA message."""
        return self.message_type == MessageType.DATA

    @property
    def is_error(self) -> bool:
        """Check if this is an ERROR message."""
        return self.message_type == MessageType.ERROR

    @property
    def is_end_of_stream(self) -> bool:
        """Check if this is an END_OF_STREAM message."""
        return self.message_type == MessageType.END_OF_STREAM
