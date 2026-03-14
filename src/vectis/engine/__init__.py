"""Vectis Engine for pipeline execution.

This module provides the Engine class and supporting components for
running pipelines from YAML configuration.
"""

from .context import WorkerContext
from .engine import Engine
from .topology import ResolvedChannel, TopologyResolver

__all__ = [
    "Engine",
    "ResolvedChannel",
    "TopologyResolver",
    "WorkerContext",
]
