"""Distributed Pipeline Example.

A pipeline demonstrating multi-worker distributed execution with:
- Worker configuration for process/host separation
- ZMQ transport for cross-process communication
- Competing distribution for load balancing
- force_inprocess for local debugging

Usage:
    # Run locally (all components in one process)
    python -m examples.distributed_example.run

    # In production, run workers separately
    python -m examples.distributed_example.run --worker producer
    python -m examples.distributed_example.run --worker consumer1
    python -m examples.distributed_example.run --worker consumer2

Or import components directly:
    from examples.distributed_example.components import DistributedProducer, DistributedConsumer
"""

from .components import DistributedConsumer, DistributedProducer

__all__ = ["DistributedProducer", "DistributedConsumer"]
