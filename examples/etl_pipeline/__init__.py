"""ETL Pipeline Example.

A multi-stage pipeline demonstrating:
- DataProvider as data source
- Algorithm with ProcessorMixin for transformation (receives and forwards)
- Algorithm as final data loader (sink)

Usage:
    python -m examples.etl_pipeline.run

Or import components directly:
    from examples.etl_pipeline.components import DataSource, Transformer, Loader
"""

from .components import DataSource, Loader, Transformer

__all__ = ["DataSource", "Transformer", "Loader"]
