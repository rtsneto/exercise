"""
Utility functions
"""

import os
import sys

import yaml
from delta.tables import DeltaTable
from loguru import logger
from pydantic import ValidationError
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

import exercise.config.main as cfg
import exercise.plugins.udfs as udf_functions
from exercise.plugins.models import SpecType


def load_and_validate_spec(spec_name: str) -> tuple[SpecType, any] | None:
    """Load and validate spec from a YAML file"""
    # Load spec configuration
    spec = load_spec_config(spec_name)
    if spec is None:
        return None

    # Validate the spec
    if not validate_spec(spec):
        return None

    spec_type = next(iter(spec))
    return (spec_type, cfg.SPEC_MODELS[spec_type](**spec[spec_type]))


def load_spec_config(spec_name: str) -> dict[str, any] | None:
    """Load spec configuration from a YAML file"""
    # Check if the spec name exists
    spec_folder = spec_name.split("-")[0]
    spec_path = f"{cfg.SPECS_PATH}{spec_folder}/{spec_name}.yaml"
    if not os.path.exists(spec_path):
        logger.error(f"Spec file not found: {spec_path}")
        return None

    try:
        with open(spec_path, "r") as file:
            return yaml.safe_load(file)
    except yaml.YAMLError as e:
        logger.error(f"Error loading YAML file {spec_path}: {e}")
        return None


def validate_spec(spec: dict[str, any]) -> bool:
    """Validate the spec against the expected schema"""
    try:
        spec_type = next(iter(spec))
        cfg.SPEC_MODELS[spec_type](**spec[spec_type])
    except ValidationError as e:
        logger.error(f"Spec validation error: {e}")
        return False

    return True


def load_spark_config_and_create_session(config_name: str) -> SparkSession | None:
    """Load Spark configuration and create a Spark session"""
    spark_config = load_spark_config(config_name)
    if not spark_config:
        return None

    return create_spark_session(spark_config)


def create_spark_session(spark_config: dict[str, any]) -> SparkSession:
    """Create a Spark session from the provided configuration"""
    builder = SparkSession.builder

    for key, value in spark_config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()


def load_spark_config(config_name: str) -> dict[str, any] | None:
    """Load Spark configuration from a YAML file"""
    try:
        with open(cfg.SPARK_CONFIG_PATH, "r") as file:
            return yaml.safe_load(file)[config_name]
    except yaml.YAMLError as e:
        logger.error(f"Error loading Spark config file {cfg.SPARK_CONFIG_PATH}: {e}")
        return None


def register_spark_udfs(spark: SparkSession, udfs: list[str]) -> None:
    """Register UDFs in the Spark session"""
    if not udfs:
        logger.info("No UDFs to register")
        return

    try:
        for udf in [getattr(udf_functions, name.lower()) for name in udfs]:
            spark.udf.register(udf.__name__, udf)
            logger.info(f"Registered UDF: {udf.__name__}")
    except Exception as e:
        logger.error(f"Error registering UDFs: {e}")
        sys.exit(1)


def load_delta_table(
    spark: SparkSession, table_path: str, schema: str = None
) -> DataFrame:
    """Load a Delta table from the specified path"""

    # If delta table does not exist, create an empty one
    if not DeltaTable.isDeltaTable(spark, table_path):
        logger.info(f"Delta table {table_path} does not exist. Creating empty one.")

        (
            spark.createDataFrame([], schema)
            .write.format("delta")
            .mode("overwrite")
            .save(table_path)
        )

    try:
        return spark.read.format("delta").load(table_path)
    except Exception as e:
        logger.error(f"Error loading delta table from {table_path}: {e}")
        sys.exit(1)


def write_merge_dataframe(
    spark: SparkSession, df: DataFrame, spec: dict[str, any]
) -> None:
    """Write the Spark dataframe to the output table using merge mode"""
    df.createOrReplaceTempView("SOURCE")
    load_delta_table(
        spark, spec.output_table, spec.output_schema
    ).createOrReplaceTempView("TARGET")

    using_on_columns = " AND ".join(
        f"TARGET.{col} = SOURCE.{col}" for col in spec.merge_columns
    )

    query = f"""
        MERGE INTO TARGET
        USING SOURCE ON {using_on_columns}
        WHEN NOT MATCHED THEN INSERT *
    """

    try:
        spark.sql(query)
        logger.info(f"Data merged to output table: {spec.output_table}")
    except Exception as e:
        logger.error(f"Error merging data to output table {spec.output_table}: {e}")
        sys.exit(1)
