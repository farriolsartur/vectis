#!/usr/bin/env python3
"""Run the order enrichment pipeline.

This script demonstrates stream joins in Vectis by running a pipeline
that joins data from three sources (orders, customers, inventory) using
a shared correlation key (order_id).

Usage:
    python run.py
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

# Import components to register them with the global registry
import components  # noqa: F401

from vectis import Engine


async def main() -> None:
    """Run the order enrichment pipeline."""
    # Get path to pipeline configuration
    config_path = Path(__file__).parent / "pipeline.yaml"

    print("=" * 60)
    print("Order Enrichment Pipeline - Stream Joins Demo")
    print("=" * 60)
    print()
    print("This pipeline demonstrates Vectis stream joins:")
    print("  - 3 data providers: orders, customers, inventory")
    print("  - 1 joiner: correlates by order_id")
    print("  - 1 processor: handles enriched orders")
    print()
    print("-" * 60)
    print("Processing orders...")
    print("-" * 60)

    # Create and run the engine
    engine = Engine(str(config_path))

    try:
        await engine.run()
    except KeyboardInterrupt:
        print("\nShutdown requested...")
        await engine.shutdown()

    # Print statistics
    print()
    print("-" * 60)
    print("Pipeline Statistics")
    print("-" * 60)

    # Access joiner statistics
    enricher = engine.components.get("order_enricher")
    if enricher:
        print(f"  Joined orders:  {enricher.joined_count}")
        print(f"  Partial joins:  {enricher.partial_count}")

    # Access processor statistics
    processor = engine.components.get("enriched_processor")
    if processor:
        print(f"  Processed:      {processor.processed_count}")
        print(f"  Fulfillable:    {processor.fulfillable_count}")

    print()
    print("=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)
