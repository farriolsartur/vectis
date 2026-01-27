"""ETL Pipeline Payload Models.

This module defines Pydantic models for data flowing through the ETL pipeline.
Using typed payloads provides:
- Validation at component boundaries
- Self-documenting data contracts
- IDE autocompletion and type checking
- Clear schema evolution tracking

Payload flow:
    DataSource emits RawRecord
    → Transformer validates, transforms, emits TransformedRecord
    → Loader receives TransformedRecord
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class RawRecord(BaseModel):
    """Raw record as extracted from the data source.

    This represents unprocessed data before transformation.
    Some records may have invalid/missing values.
    """

    id: int = Field(ge=1, description="Sequential record identifier")
    name: str = Field(min_length=1, description="Item name")
    value: int | None = Field(default=None, description="Value (None = invalid)")
    category: str = Field(pattern="^[AB]$", description="Category A or B")


class TransformedRecord(BaseModel):
    """Record after transformation and enrichment.

    This represents validated, cleaned data ready for loading.
    All fields are guaranteed to be valid.
    """

    id: int = Field(ge=1)
    name: str = Field(min_length=1, description="Processed name (may be uppercased)")
    value: int = Field(ge=0, description="Validated value (never None)")
    category: str = Field(pattern="^[AB]$")
    source: str = Field(description="Source component that produced the raw record")
    processed_at: float | None = Field(
        default=None, description="Unix timestamp when processed"
    )
