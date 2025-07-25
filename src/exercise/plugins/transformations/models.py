"""
Pydantic models for transformations
"""

from pydantic import BaseModel, Field, model_validator

from exercise.plugins.models import InputTable, WriteMode


class TransformationSpec(BaseModel):
    """
    Model for CSV transformations
    """

    name: str = Field(..., description="Name of the transformation")

    input_tables: list[InputTable] = Field(
        ...,
        description="List of input tables (folder paths, schema...) for the transformation",
    )
    output_table: str = Field(
        ..., description="Output table (folder path) for transformation"
    )
    output_schema: str = Field(..., description="Schema of the output table")
    write_mode: WriteMode = Field(
        ...,
        description="Write mode for the transformation: append, merge, overwrite",
    )
    sql_query: str = Field(
        ...,
        description="SQL query to run on the input data",
    )
    cluster_by: list[str] = Field(
        [],
        description="List of columns to cluster by the data",
    )
    merge_columns: list[str] = Field(
        [],
        description="List of columns to merge on",
    )
    udfs: list[str] = Field(
        [],
        description="List of UDFs to register",
    )
    spark_config: str = Field(
        ..., description="Spark configuration used for the transformation"
    )

    @model_validator(mode="after")
    def check_merge_columns_for_merge(self):
        if self.write_mode == WriteMode.MERGE and len(self.merge_columns) == 0:
            raise ValueError(
                "You must specify at least one `merge_columns` when write_mode='merge'"
            )
        return self
