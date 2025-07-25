"""
Helper functions for CSV transformations
"""

from pyspark.sql import SparkSession

from exercise.plugins.transformations.csv_transformation.models import (
    TransformationCSVSpec,
)


def process_csv_transformation(
    spark: SparkSession, spec: TransformationCSVSpec
) -> None:
    """Process CSV transformation using Spark"""
    # Read input tables
    for table in spec.input_tables:
        df = (
            spark.read.option("header", table.header)
            .option("delimiter", table.delimiter)
            .option("encoding", table.encoding)
            .option("dateFormat", table.date_format)
            .option("timestampFormat", table.timestamp_format)
            .schema(table.schema)
            .csv(table.path)
        )

        df.createOrReplaceTempView(table.name)

    return spark.sql(spec.sql_query)
