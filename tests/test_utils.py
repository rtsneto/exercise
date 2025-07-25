"""
Utility functions tests
"""

import os
import shutil
from types import SimpleNamespace

import pytest
from pyspark.testing.utils import assertDataFrameEqual

import exercise.config.main as cfg
from exercise.plugins.utils import (
    create_spark_session,
    load_and_validate_spec,
    load_delta_table,
    load_spark_config,
    load_spec_config,
    register_spark_udfs,
    validate_spec,
    write_merge_dataframe,
)


@pytest.mark.parametrize("spec_file", [
    fname for fname in os.listdir(os.path.join(cfg.SPECS_PATH, "exercise"))
    if fname.endswith(".yaml")
])
def test_load_and_validate_spec_for_all_yaml(spec_file):
    """Test that all YAML spec files can be loaded and validated"""
    spec_name = spec_file.replace(".yaml", "")
    result = load_and_validate_spec(spec_name)
    spec_type, spec_obj = result

    assert result is not None, f"Failed to load and validate spec: {spec_name}"
    assert spec_type in cfg.SPEC_MODELS, f"Spec type {spec_type} not in SPEC_MODELS"
    assert isinstance(spec_obj, cfg.SPEC_MODELS[spec_type]), f"Spec object is not instance of expected model for {spec_type}"


@pytest.mark.parametrize("spec_file", [
    fname for fname in os.listdir(os.path.join(cfg.SPECS_PATH, "exercise"))
    if fname.endswith(".yaml")
])
def test_load_spec_config_for_all_yaml(spec_file):
    """Test that all YAML spec files can be loaded correctly"""
    spec_name = spec_file.replace(".yaml", "")
    result = load_spec_config(spec_name)

    assert result is not None, f"Failed to load spec: {spec_name}"
    assert isinstance(result, dict), f"Spec {spec_name} did not return a dict"


@pytest.mark.parametrize("spec_file", [
    fname for fname in os.listdir(os.path.join(cfg.SPECS_PATH, "exercise"))
    if fname.endswith(".yaml")
])
def test_validate_spec_for_all_yaml(spec_file):
    """Test that all YAML spec files are valid"""
    spec_name = spec_file.replace(".yaml", "")
    spec = load_spec_config(spec_name)

    assert spec is not None, f"Failed to load spec: {spec_name}"
    assert validate_spec(spec), f"Spec validation failed for: {spec_name}"


def test_create_spark_session_and_register_udf():
    """Test that a Spark session can be created and UDFs can be registered"""
    spark_config = load_spark_config("spark-small")
    spark = create_spark_session(spark_config)

    udf_to_register = "generate_nse_id"
    register_spark_udfs(spark, [udf_to_register])
    all_funcs = spark.catalog.listFunctions()

    assert any(func.name == udf_to_register for func in all_funcs), f"UDF {udf_to_register} not registered in Spark session"


def test_load_delta_table_empty(tmp_path):
    """Test that load_delta_table creates an empty delta table if it does not exist"""
    spark = create_spark_session(load_spark_config("spark-small"))
    table_path = f"{tmp_path}/empty_delta_table"
    schema = "id INT, name STRING"

    # Clean up any existing output path
    shutil.rmtree(table_path, ignore_errors=True)

    # Load delta table (should create an empty one)
    df = load_delta_table(spark, table_path, schema)

    # Check that the dataframe is empty, with a correct schema
    assert df.count() == 0
    assert dict(df.dtypes) == {"id": "int", "name": "string"}

def test_load_delta_table_non_empty(tmp_path):
    """Test that load_delta_table reads an existing delta table with data"""
    spark = create_spark_session(load_spark_config("spark-small"))
    table_path = f"{tmp_path}/non_empty_delta_table"
    schema = "id INT, name STRING"

    # Clean up any existing output path
    shutil.rmtree(table_path, ignore_errors=True)

    # Create new delta table
    target_data = [(1, "aaa"), (2, "bbb")]
    (
        spark
        .createDataFrame(target_data, schema=schema)
        .write.format("delta").mode("overwrite").save(table_path)
    )

    # Load delta table
    result_df = load_delta_table(spark, table_path, schema)

    # Create an expected dataframe with 2 rows
    expected_data = [(1, "aaa"), (2, "bbb")]
    expected_df = spark.createDataFrame(expected_data, schema=schema)

    # Compare the loaded dataframe with the expected one
    assertDataFrameEqual(
        result_df,
        expected_df,
        checkRowOrder=False,
        ignoreColumnOrder=True
    )






def test_write_merge_dataframe(tmp_path):
    """Test the write_merge_dataframe function with a Delta table"""
    spark = create_spark_session(load_spark_config("spark-small"))
    table_path = f"{tmp_path}/delta_table"
    schema = "id INT, name STRING"

    # Create delta table with 2 rows
    target_data = [(1, "aaa"), (2, "bbb")]
    target_df = spark.createDataFrame(target_data, schema=schema)

    # Clean up any existing table path
    shutil.rmtree(table_path, ignore_errors=True)

    # Write the initial data to the delta table
    target_df.write.format("delta").mode("overwrite").save(table_path)

    # Create new data with 2 rows (1 new unique row) to merge
    source_data = [(2, "bbb"), (3, "ccc")]
    source_df = spark.createDataFrame(source_data, schema=schema)

    # Create a spec for the merge using a SimpleNamespace for attribute access
    spec = SimpleNamespace(
        output_table=table_path,
        output_schema=schema,
        merge_columns=["id"],
    )

    # Merge new data into the delta table
    write_merge_dataframe(spark, source_df, spec)

    # Read the delta table back and check that we now have 3 rows after the merge
    result_df = spark.read.format("delta").load(table_path)

    # Create an expected dataframe with 3 rows
    expected_data = [(1, "aaa"), (2, "bbb"), (3, "ccc")]
    expected_df = spark.createDataFrame(expected_data, schema=schema)

    # Compare the result dataframe with the expected one
    assertDataFrameEqual(
        result_df,
        expected_df,
        checkRowOrder=False,
        ignoreColumnOrder=True
    )
