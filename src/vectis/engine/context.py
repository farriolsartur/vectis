"""Vectis worker context for runtime state management.

This module provides the WorkerContext dataclass that encapsulates
runtime state for a worker's execution of pipeline components.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vectis.config.models import (
        ComponentInstanceConfig,
        PipelineConfig,
    )


@dataclass
class WorkerContext:
    """Runtime context for a worker's execution.

    WorkerContext encapsulates the state needed for a worker to execute
    its portion of the pipeline. For single-process pipelines (MVP),
    there is one WorkerContext containing all components.

    Attributes:
        worker_name: Unique identifier for this worker.
        pipeline_config: The full pipeline configuration.
        force_inprocess: Whether to force all channels to be in-process.

    Example:
        >>> context = WorkerContext(
        ...     worker_name="main",
        ...     pipeline_config=config,
        ...     force_inprocess=False,
        ... )
        >>> for component in context.get_components():
        ...     print(component.name)
    """

    worker_name: str
    pipeline_config: PipelineConfig
    force_inprocess: bool = False
    _component_instances: list[ComponentInstanceConfig] | None = field(
        default=None, repr=False
    )

    def get_components(self) -> list[ComponentInstanceConfig]:
        """Get component instances assigned to this worker.

        If force_inprocess=True, returns ALL components regardless of worker assignment.
        Returns cached result on subsequent calls.

        Returns:
            List of ComponentInstanceConfig for this worker.
        """
        if self._component_instances is not None:
            return self._component_instances

        instances: list[ComponentInstanceConfig] = []

        # If force_inprocess, get ALL components (ignore worker filtering)
        if self.force_inprocess:
            for component_list in self.pipeline_config.components_by_type.values():
                instances.extend(component_list)
        else:
            # Normal worker filtering
            for component_list in self.pipeline_config.components_by_type.values():
                for instance in component_list:
                    # Include if assigned to this worker, or unassigned
                    if instance.worker == self.worker_name or instance.worker is None:
                        instances.append(instance)

        self._component_instances = instances
        return instances

    def get_component_type(self, instance_name: str) -> str | None:
        """Get the component type (section name) for an instance.

        Args:
            instance_name: The name of the component instance.

        Returns:
            The component type (e.g., 'data_provider', 'algorithm'), or None.
        """
        return self.pipeline_config.get_component_type(instance_name)
