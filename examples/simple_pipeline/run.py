#!/usr/bin/env python3
"""Run the simple pipeline example.

Usage:
    python -m examples.simple_pipeline.run
    # or from project root:
    python examples/simple_pipeline/run.py
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
from examples.simple_pipeline.components import CounterProvider, PrinterAlgorithm  # noqa: F401

from vectis import Engine


def setup_logging() -> None:
    """Configure logging for the example."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


async def main() -> None:
    """Run the simple pipeline."""
    setup_logging()

    # Get the path to the pipeline configuration
    config_path = Path(__file__).parent / "pipeline.yaml"

    print("=" * 50)
    print("Vectis Simple Pipeline Example")
    print("=" * 50)
    print(f"Config: {config_path}")
    print()

    # Create and run the engine
    engine = Engine(str(config_path))

    try:
        await engine.run()
    except KeyboardInterrupt:
        print("\nInterrupted by user")

    # Access component state after execution
    printer = engine.components.get("printer")
    if printer:
        print()
        print("=" * 50)
        print(f"Pipeline completed. Printer received {printer.received_count} messages.")
        print(f"Values: {[v.get('value') for v in printer.received_values]}")
        print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
