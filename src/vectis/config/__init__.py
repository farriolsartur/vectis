"""Vectis configuration system for YAML pipeline definitions.

This module provides tools for defining pipelines declaratively in YAML:

- Config Models: Pydantic models for pipeline configuration
- ConfigLoader: Load and validate YAML configuration files

Example:
    >>> from vectis.config import ConfigLoader, PipelineConfig
    >>>
    >>> loader = ConfigLoader()
    >>> config = loader.load("pipeline.yaml")
    >>> errors = loader.validate(config)
    >>> if not errors:
    ...     # config is ready for engine
    ...     pass
"""

from .loader import ConfigLoader
from .models import (
    BackpressureConfig,
    ComponentInstanceConfig,
    ConnectionConfig,
    DefaultsConfig,
    GlobalConfig,
    PipelineConfig,
    TransportConfig,
    WorkerConfig,
)

__all__ = [
    # Loader
    "ConfigLoader",
    # Models
    "BackpressureConfig",
    "ComponentInstanceConfig",
    "ConnectionConfig",
    "DefaultsConfig",
    "GlobalConfig",
    "PipelineConfig",
    "TransportConfig",
    "WorkerConfig",
]
