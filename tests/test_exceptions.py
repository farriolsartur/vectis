"""Tests for Vectis exception types."""

from __future__ import annotations

import pytest

from vectis import ComponentNotFoundError, VectisError, PipelineConfigError


class TestVectisError:
    """Tests for the base VectisError exception."""

    def test_is_exception(self) -> None:
        """VectisError should be an Exception subclass."""
        assert issubclass(VectisError, Exception)

    def test_can_raise_and_catch(self) -> None:
        """VectisError can be raised and caught."""
        with pytest.raises(VectisError) as exc_info:
            raise VectisError("test error")
        assert str(exc_info.value) == "test error"


class TestPipelineConfigError:
    """Tests for PipelineConfigError exception."""

    def test_is_vectis_error(self) -> None:
        """PipelineConfigError should be a VectisError subclass."""
        assert issubclass(PipelineConfigError, VectisError)

    def test_can_raise_with_message(self) -> None:
        """PipelineConfigError can be raised with a message."""
        with pytest.raises(PipelineConfigError) as exc_info:
            raise PipelineConfigError("Invalid YAML syntax")
        assert "Invalid YAML syntax" in str(exc_info.value)

    def test_caught_as_vectis_error(self) -> None:
        """PipelineConfigError can be caught as VectisError."""
        with pytest.raises(VectisError):
            raise PipelineConfigError("config error")


class TestComponentNotFoundError:
    """Tests for ComponentNotFoundError exception."""

    def test_is_vectis_error(self) -> None:
        """ComponentNotFoundError should be a VectisError subclass."""
        assert issubclass(ComponentNotFoundError, VectisError)

    def test_stores_component_name(self) -> None:
        """ComponentNotFoundError should store the component name."""
        error = ComponentNotFoundError("my_algorithm")
        assert error.component_name == "my_algorithm"

    def test_default_message(self) -> None:
        """ComponentNotFoundError generates a default message."""
        error = ComponentNotFoundError("my_algorithm")
        assert "my_algorithm" in str(error)
        assert "not found" in str(error).lower()

    def test_custom_message(self) -> None:
        """ComponentNotFoundError accepts a custom message."""
        error = ComponentNotFoundError(
            "my_algorithm",
            message="Custom error: my_algorithm is not registered"
        )
        assert "Custom error" in str(error)

    def test_caught_as_vectis_error(self) -> None:
        """ComponentNotFoundError can be caught as VectisError."""
        with pytest.raises(VectisError):
            raise ComponentNotFoundError("missing_component")
