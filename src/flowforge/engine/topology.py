"""FlowForge topology resolver for determining channel configurations.

This module provides the TopologyResolver class that takes pipeline
configuration and resolves connections into concrete channel specifications
with all settings merged from defaults.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from flowforge.communication.enums import (
    BackpressureMode,
    CompetingStrategy,
    DistributionMode,
    TransportType,
)

if TYPE_CHECKING:
    from flowforge.config.models import PipelineConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ResolvedChannel:
    """Resolved channel configuration for a connection.

    This represents a fully-resolved connection with all settings determined
    (either from explicit configuration or defaults).

    Attributes:
        source: Name of the sending component.
        target: Receiving component name.
        transport_type: How messages are transported (INPROCESS for MVP).
        distribution_mode: FAN_OUT or COMPETING distribution.
        strategy: Strategy for competing distribution (ROUND_ROBIN or RANDOM).
        queue_size: Maximum queue size for backpressure handling.
        serialization: Serializer name (json or msgpack).
        endpoint: Network endpoint for distributed transport.
        backpressure_mode: Backpressure handling mode (block or drop).
    """

    source: str
    target: str
    transport_type: TransportType
    distribution_mode: DistributionMode
    strategy: CompetingStrategy
    queue_size: int
    serialization: str = "json"
    endpoint: str | None = None
    backpressure_mode: BackpressureMode = BackpressureMode.BLOCK


class TopologyResolver:
    """Resolves pipeline connections into channel configurations.

    The TopologyResolver takes a PipelineConfig and optional worker name,
    and produces ResolvedChannel objects that specify exactly how to
    create channels for each connection.

    For the MVP (Phase 5), all channels are INPROCESS. Phase 6 adds
    logic to determine MULTIPROCESS or DISTRIBUTED per target based on
    component placement across workers.

    Example:
        >>> resolver = TopologyResolver()
        >>> channels = resolver.resolve(config, worker_name=None)
        >>> for channel in channels:
        ...     print(f"{channel.source} -> {channel.target}")
    """

    def resolve(
        self,
        config: PipelineConfig,
        worker_name: str | None = None,
        force_inprocess: bool = False,
    ) -> list[ResolvedChannel]:
        """Resolve pipeline connections to channel configurations.

        Args:
            config: The pipeline configuration.
            worker_name: Optional worker to filter components for.
                        If None, includes all components (single-process mode).
            force_inprocess: If True, all channels are INPROCESS regardless
                           of worker placement (for local debugging).

        Returns:
            List of ResolvedChannel configurations.
        """
        resolved: list[ResolvedChannel] = []
        defaults = config.global_config.defaults

        # Get components for this worker (or all if force_inprocess)
        worker_components = self._get_worker_components(
            config,
            worker_name,
            force_inprocess,
        )
        component_workers = self._get_component_workers(config, worker_name)
        worker_hosts = {w.name: w.host for w in config.workers}

        for connection in config.connections:
            # Filter: include if source or any target is in this worker
            # Skip filtering entirely when force_inprocess=True
            if worker_name and not force_inprocess:
                source_in_worker = connection.source in worker_components
                targets_in_worker = {
                    target for target in connection.targets if target in worker_components
                }
                if not source_in_worker and not targets_in_worker:
                    continue

            # Resolve settings with defaults
            distribution = connection.distribution or defaults.distribution
            strategy = connection.strategy or defaults.strategy
            serialization = connection.serialization or defaults.serialization
            backpressure_mode = (
                connection.backpressure.mode
                if connection.backpressure
                else defaults.backpressure.mode
            )
            queue_size = (
                connection.backpressure.queue_size
                if connection.backpressure
                else defaults.backpressure.queue_size
            )

            # Log if force_inprocess is used with workers configured
            if force_inprocess and config.workers:
                logger.warning(
                    "force_inprocess=True: connection %s -> %s using INPROCESS "
                    "(worker placement ignored for local debugging)",
                    connection.source,
                    connection.targets,
                )

            for target in connection.targets:
                # Per-worker filtering for targets
                if worker_name and not force_inprocess:
                    if connection.source not in worker_components and target not in worker_components:
                        continue

                transport_type = self._determine_transport_type(
                    connection.source,
                    target,
                    component_workers,
                    worker_hosts,
                    worker_name,
                    force_inprocess,
                    bool(config.workers),
                )

                endpoint = None
                if transport_type == TransportType.DISTRIBUTED:
                    target_worker = component_workers.get(target) or worker_name
                    target_host = (
                        worker_hosts.get(target_worker, "localhost")
                        if target_worker
                        else "localhost"
                    )

                    # Get fixed port if specified
                    fixed_port = None
                    if connection.ports and target in connection.ports:
                        fixed_port = connection.ports[target]

                    endpoint = self._generate_endpoint(
                        connection.source,
                        target,
                        target_host,
                        config,
                        fixed_port=fixed_port,
                    )

                resolved.append(
                    ResolvedChannel(
                        source=connection.source,
                        target=target,
                        transport_type=transport_type,
                        distribution_mode=distribution,
                        strategy=strategy,
                        queue_size=queue_size,
                        serialization=serialization,
                        endpoint=endpoint,
                        backpressure_mode=backpressure_mode,
                    )
                )

        return resolved

    def _get_worker_components(
        self,
        config: PipelineConfig,
        worker_name: str | None,
        force_inprocess: bool = False,
    ) -> set[str]:
        """Get component instance names assigned to a worker.

        If force_inprocess=True, returns all components (ignores worker filtering).

        Args:
            config: The pipeline configuration.
            worker_name: Worker name to filter by, or None for all.
            force_inprocess: If True, return all components regardless of worker.

        Returns:
            Set of component instance names.
        """
        if worker_name is None or force_inprocess:
            # All components when no worker specified OR force_inprocess
            return set(config.get_all_component_instances().keys())

        # Filter by worker assignment
        components: set[str] = set()
        for instances in config.components_by_type.values():
            for instance in instances:
                # Include if assigned to this worker, or unassigned (defaults to any worker)
                if instance.worker == worker_name or instance.worker is None:
                    components.add(instance.name)
        return components

    def _get_component_workers(
        self,
        config: PipelineConfig,
        worker_name: str | None,
    ) -> dict[str, str | None]:
        """Map component instance name to worker assignment."""
        mapping: dict[str, str | None] = {}
        for instances in config.components_by_type.values():
            for instance in instances:
                mapping[instance.name] = instance.worker or worker_name
        return mapping

    def _determine_transport_type(
        self,
        source: str,
        target: str,
        component_workers: dict[str, str | None],
        worker_hosts: dict[str, str],
        worker_name: str | None,
        force_inprocess: bool,
        has_workers: bool,
    ) -> TransportType:
        """Determine transport type for a source/target pair."""
        if force_inprocess or not has_workers:
            return TransportType.INPROCESS

        source_worker = component_workers.get(source) or worker_name
        target_worker = component_workers.get(target) or worker_name

        if source_worker == target_worker:
            return TransportType.INPROCESS

        source_host = worker_hosts.get(source_worker or "", "localhost")
        target_host = worker_hosts.get(target_worker or "", "localhost")
        if source_host == target_host:
            return TransportType.MULTIPROCESS
        return TransportType.DISTRIBUTED

    def _generate_endpoint(
        self,
        source: str,
        target: str,
        target_host: str,
        config: PipelineConfig,
        fixed_port: int | None = None,
    ) -> str:
        """Generate a stable endpoint for a distributed connection.

        Args:
            source: Source component name.
            target: Target component name.
            target_host: Host where target component runs.
            config: Pipeline configuration.
            fixed_port: Optional fixed port override. If provided, skips
                hash-based port generation.

        Returns:
            Endpoint URL string (e.g., "tcp://host:port").
        """
        transport = config.global_config.transport
        transport_cfg = transport.config if transport else {}

        protocol = transport_cfg.get("protocol", "tcp")
        template = transport_cfg.get("endpoint_template")

        # Determine port: use fixed port or fall back to hash
        if fixed_port is not None:
            port = fixed_port
        else:
            base_port = int(transport_cfg.get("base_port", 5555))
            port_range = int(transport_cfg.get("port_range", 1000))

            if port_range <= 0:
                port_range = 1

            key = f"{source}->{target}"
            digest = hashlib.sha256(key.encode("utf-8")).digest()
            offset = int.from_bytes(digest[:4], "big") % port_range
            port = base_port + offset

        if template:
            return template.format(
                host=target_host,
                port=port,
                source=source,
                target=target,
            )

        return f"{protocol}://{target_host}:{port}"
