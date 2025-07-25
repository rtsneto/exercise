"""
Tests for Spark configuration file validity
"""

import os

import yaml

from exercise.config.main import SPARK_CONFIG_PATH


def test_spark_config_file_exists():
    """Test that the Spark config file exists"""
    assert os.path.exists(SPARK_CONFIG_PATH), (
        f"Spark config file not found: {SPARK_CONFIG_PATH}"
    )


def test_spark_config_file_is_valid_yaml():
    """Test that the Spark config file contains valid YAML"""
    # Test that the file contains valid YAML
    try:
        with open(SPARK_CONFIG_PATH, "r") as file:
            yaml_content = yaml.safe_load(file)

        # Ensure the YAML content is not None or empty
        assert yaml_content is not None, "YAML file should not be empty"
        assert isinstance(yaml_content, dict), "YAML content should be a dictionary"
        assert len(yaml_content) > 0, "YAML should contain at least one configuration"

    except yaml.YAMLError as e:
        assert False, f"Invalid YAML in Spark config file: {e}"
    except Exception as e:
        assert False, f"Error reading Spark config file: {e}"
