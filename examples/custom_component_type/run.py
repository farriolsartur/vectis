#!/usr/bin/env python3
"""Run the custom component type example.

Usage:
    python -m examples.custom_component_type.run
    # or from project root:
    python examples/custom_component_type/run.py
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

# Import components to register them with the registries
from examples.custom_component_type.components import (  # noqa: F401
    CounterProvider,
    MultiplierProcessor,
    PrinterAlgorithm,
)

from flowforge import Engine


def setup_logging() -> None:
    """Configure logging for the example."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


async def main() -> None:
    """Run the custom component type pipeline."""
    setup_logging()

    config_path = Path(__file__).parent / "pipeline.yaml"

    print("=" * 60)
    print("FlowForge Custom Component Type Example")
    print("=" * 60)
    print(f"Config: {config_path}")
    print()
    print("Pipeline: Counter -> Multiplier (processor) -> Printer")
    print("  - Counter generates 5 integers starting at 1")
    print("  - Multiplier is a custom component type (processor)")
    print("  - Printer receives multiplied values")
    print()

    engine = Engine(str(config_path))

    try:
        await engine.run()
    except KeyboardInterrupt:
        print("\nInterrupted by user")

    # Report results
    print()
    print("=" * 60)
    print("Pipeline Results")
    print("=" * 60)

    counter = engine.components.get("counter")
    multiplier = engine.components.get("multiplier")
    printer = engine.components.get("printer")

    if counter:
        print(f"Counter sent:     {counter.sent_values}")
    if multiplier:
        print(f"Multiplier output: {multiplier.processed_values}")
    if printer:
        values = [v.get("value") for v in printer.received_values]
        print(f"Printer received: {values}")


if __name__ == "__main__":
    asyncio.run(main())
