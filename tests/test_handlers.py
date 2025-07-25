"""
Handlers tests
"""

import shutil
from types import SimpleNamespace

from pyspark.testing.utils import assertDataFrameEqual

from exercise.plugins.transformations.csv_transformation.handlers import (
    process_csv_transformation,
)
from exercise.plugins.utils import create_spark_session, load_spark_config


def test_process_csv_transformation(tmp_path):
    """Test the CSV transformation logic end‑to‑end"""
    spark = create_spark_session(load_spark_config("spark-small"))
    source_schema = "id INT, name STRING, age INT"
    target_schema = "id INT, name STRING, age_some_time_in_the_past INT"
    table_path = tmp_path / "people.csv"

    # Clean up any existing table path and create a new one
    shutil.rmtree(table_path, ignore_errors=True)

    # Create a CSV file with sample data
    table_path.write_text("id,name,age\n1,aaa,50\n2,bbb,30\n3,ccc,40\n")

    # Create a spec for the CSV transformation
    csv_spec = SimpleNamespace(
        name="people",
        path=str(table_path),
        schema=source_schema,
        header=True,
        delimiter=",",
        encoding="UTF-8",
        date_format=None,
        timestamp_format=None,
    )
    spec = SimpleNamespace(
        input_tables=[csv_spec],
        sql_query="""
            SELECT
                id,
                name,
                age - 5 AS age_some_time_in_the_past
            FROM people
        """,
    )

    # Run the transformation
    result_df = process_csv_transformation(spark, spec)

    # Create an expected dataframe with age changed
    expected_df = spark.createDataFrame(
        [(1, "aaa", 45), (2, "bbb", 25), (3, "ccc", 35)],
        schema=target_schema,
    )

    # Compare the result DataFrame with the expected one
    assertDataFrameEqual(
        result_df,
        expected_df,
        checkRowOrder=False,
        ignoreColumnOrder=True,
    )
