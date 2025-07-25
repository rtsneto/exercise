"""
Pydantic models for CSV transformations
"""

from pydantic import Field

from exercise.plugins.models import InputTable
from exercise.plugins.transformations.models import TransformationSpec


class CSVInputTable(InputTable):
    header: bool = Field(True, description="Whether the CSV has a header row")
    delimiter: str = Field(",", description="Delimiter character")
    encoding: str = Field("utf-8", description="File encoding")
    date_format: str = Field(
        "dd.MM.yyyy", description="Date format for parsing dates in the CSV"
    )
    timestamp_format: str = Field(
        "dd.MM.yyyy HH:mm",
        description="Timestamp format for parsing timestamps in the CSV",
    )


class TransformationCSVSpec(TransformationSpec):
    input_tables: list[CSVInputTable] = Field(
        ...,
        description="List of CSV input tables (folder paths, schema, header...) for the transformation",
    )
