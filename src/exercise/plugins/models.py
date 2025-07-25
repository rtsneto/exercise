"""
Pydantic models for extractions and transformations
"""

from enum import StrEnum

from pydantic import BaseModel, Field


class SpecType(StrEnum):
    """
    Enum for different specs (transformation, extraction, etc.)
    """

    TRANSFORMATION_CSV = "transformation-csv"


class SourceType(StrEnum):
    """
    Enum for different types of transformations
    """

    CSV = "csv"


class InputTable(BaseModel):
    name: str = Field(..., description="SQL view name for the input source")
    path: str = Field(..., description="Path of the input source")
    schema: str = Field(..., description="Schema of the input source")


class WriteMode(StrEnum):
    """
    Enum for different write modes
    """

    MERGE = "merge"
