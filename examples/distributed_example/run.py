#!/usr/bin/env python3
"""Run the distributed pipeline example.

This example demonstrates distributed pipeline execution. It can be run in
two modes:

1. Local mode (force_inprocess=True): All components run in a single process.
   Useful for development and debugging.

2. Distributed mode: Each worker runs in a separate process (production).
   Requires running the script multiple times with different --worker flags.

Usage:
    # Local mode (default) - for development/testing
    python -m examples.distributed_example.run
    # or from project root:
    python examples/distributed_example/run.py

    # Distributed mode - run each worker in a separate terminal
    python -m examples.distributed_example.run --worker producer
    python -m examples.distributed_example.run --worker consumer1
    python -m examples.distributed_example.run --worker consumer2
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path when running directly
_project_root = Path(__file__).parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Import components to register them with the ComponentRegistry
from examples.distributed_example.components import (  # noqa: F401
    DistributedConsumer,
    DistributedProducer,
)

from flowforge import Engine


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the example."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the distributed pipeline example"
    )
    parser.add_argument(
        "--worker",
        type=str,
        default=None,
        help="Worker name to run (producer, consumer1, consumer2). "
             "If not specified, runs all components locally (force_inprocess).",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args()


async def main() -> None:
    """Run the distributed pipeline."""
    args = parse_args()
    setup_logging(args.verbose)

    # Get the path to the pipeline configuration
    config_path = Path(__file__).parent / "pipeline.yaml"

    # Determine execution mode
    force_inprocess = args.worker is None

    print("=" * 60)
    print("FlowForge Distributed Pipeline Example")
    print("=" * 60)
    print(f"Config: {config_path}")
    print()

    if force_inprocess:
        print("Mode: LOCAL (all components in one process)")
        print("      Use --worker <name> for distributed execution")
        print()
        print("Pipeline: Producer -> [Consumer1, Consumer2] (competing)")
        print("  - Producer generates 50 work items (5 batches x 10 items)")
        print("  - Items are load-balanced across consumers (round-robin)")
        print()
        engine = Engine(str(config_path))
    else:
        print(f"Mode: DISTRIBUTED (running worker: {args.worker})")
        print()
        print("Make sure to start all workers:")
        print("  python -m examples.distributed_example.run --worker producer")
        print("  python -m examples.distributed_example.run --worker consumer1")
        print("  python -m examples.distributed_example.run --worker consumer2")
        print()
        engine = Engine(str(config_path), worker_name=args.worker)

    try:
        await engine.run(force_inprocess=force_inprocess)
    except KeyboardInterrupt:
        print("\nInterrupted by user")

    # Report results (only meaningful in local mode)
    if force_inprocess:
        print()
        print("=" * 60)
        print("Pipeline Results")
        print("=" * 60)

        producer = engine.components.get("producer")
        consumer1 = engine.components.get("consumer1")
        consumer2 = engine.components.get("consumer2")

        if producer:
            print(f"Producer:  {producer.items_produced} items sent")

        if consumer1 and consumer2:
            total = consumer1.processed_count + consumer2.processed_count
            print(f"Consumer1: {consumer1.processed_count} items processed")
            print(f"Consumer2: {consumer2.processed_count} items processed")
            print(f"Total:     {total} items (should equal producer)")

            # Verify load balancing
            if consumer1.processed_count > 0 and consumer2.processed_count > 0:
                ratio = min(consumer1.processed_count, consumer2.processed_count) / max(
                    consumer1.processed_count, consumer2.processed_count
                )
                print(f"\nLoad balance ratio: {ratio:.2%}")
                if ratio > 0.8:
                    print("Load balancing: Good (items distributed evenly)")
                else:
                    print("Load balancing: Uneven (may be due to processing delays)")


if __name__ == "__main__":
    asyncio.run(main())
