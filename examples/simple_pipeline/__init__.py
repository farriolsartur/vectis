"""Simple Pipeline Example.

A minimal FlowForge pipeline demonstrating:
- DataProvider: Generates sequential numbers
- Algorithm: Receives and processes data

Usage:
    python -m examples.simple_pipeline.run

Or import components directly:
    from examples.simple_pipeline.components import CounterProvider, PrinterAlgorithm
"""

from .components import CounterProvider, PrinterAlgorithm

__all__ = ["CounterProvider", "PrinterAlgorithm"]
