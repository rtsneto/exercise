"""
Main script to run transformations (job name as a parameter of the script)
"""

import sys

from loguru import logger

import exercise.config.main as cfg
from exercise.plugins.models import SpecType
from exercise.plugins.transformations.csv_transformation.handlers import (
    process_csv_transformation,
)
from exercise.plugins.transformations.models import WriteMode
from exercise.plugins.utils import (
    load_and_validate_spec,
    load_spark_config_and_create_session,
    register_spark_udfs,
    write_merge_dataframe,
)

# Check if the transformation name is provided as a parameter
if len(sys.argv) > 1:
    transformation_name = sys.argv[1]
    logger.info(f"Running transformation: {transformation_name}")
else:
    logger.error("No transformation name provided as a parameter")
    sys.exit(1)

# Loguru setup
logger.add(
    f"logs/{transformation_name}.log",
    rotation=None,
    retention=cfg.LOG_RETENTION,
)

# Load and validate the transformation spec
transformation_type, spec = load_and_validate_spec(transformation_name)
if spec is None:
    logger.error(f"Failed to load or validate spec for {transformation_name}")
    sys.exit(1)

# Load Spark config and init Spark session
spark = load_spark_config_and_create_session(spec.spark_config)
if spark is None:
    logger.error("Failed to load Spark config and create Spark session")
    sys.exit(1)

# Register UDFs
register_spark_udfs(spark, spec.udfs)

# Run transformations
if transformation_type == SpecType.TRANSFORMATION_CSV:
    df_transformed = process_csv_transformation(spark, spec)

if df_transformed is None:
    logger.error(f"Failed to process transformation for {transformation_name}")
    sys.exit(1)

if df_transformed.count() == 0:
    logger.warning(f"No data found for transformation {transformation_name}")
    sys.exit(1)

# Write transformed data to output table
if spec.write_mode == WriteMode.MERGE:
    write_merge_dataframe(spark, df_transformed, spec)
