#!/usr/bin/env python3
"""Run the ETL pipeline example.

Usage:
    python -m examples.etl_pipeline.run
    # or from project root:
    python examples/etl_pipeline/run.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path when running directly
_project_root = Path(__file__).parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# Import components to register them with the ComponentRegistry
from examples.etl_pipeline.components import DataSource, Loader, Transformer  # noqa: F401

from vectis import Engine


def setup_logging() -> None:
    """Configure logging for the example."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


async def main() -> None:
    """Run the ETL pipeline."""
    setup_logging()

    # Get the path to the pipeline configuration
    config_path = Path(__file__).parent / "pipeline.yaml"

    print("=" * 60)
    print("Vectis ETL Pipeline Example")
    print("=" * 60)
    print(f"Config: {config_path}")
    print()
    print("Pipeline: DataSource -> Transformer -> Loader")
    print("  - Source generates 20 records (some invalid)")
    print("  - Transformer filters value < 20, uppercases names")
    print("  - Loader stores final records")
    print()

    # Create and run the engine
    engine = Engine(str(config_path))

    try:
        await engine.run()
    except KeyboardInterrupt:
        print("\nInterrupted by user")

    # Access component state after execution
    print()
    print("=" * 60)
    print("Pipeline Results")
    print("=" * 60)

    source = engine.components.get("source")
    transformer = engine.components.get("transformer")
    loader = engine.components.get("loader")

    if source:
        print(f"Source:      {source.records_sent} records generated")
    if transformer:
        print(f"Transformer: {transformer.processed_count} passed, "
              f"{transformer.filtered_count} filtered, "
              f"{transformer.error_count} errors")
    if loader:
        print(f"Loader:      {loader.loaded_count} records stored")

        # Show sample of loaded data (typed TransformedRecord objects)
        if loader.loaded_records:
            print()
            print("Sample loaded records (TransformedRecord):")
            for record in loader.loaded_records[:3]:
                # Display as dict for readability
                print(f"  {record.model_dump(exclude={'processed_at'})}")
            if len(loader.loaded_records) > 3:
                print(f"  ... and {len(loader.loaded_records) - 3} more")


if __name__ == "__main__":
    asyncio.run(main())
