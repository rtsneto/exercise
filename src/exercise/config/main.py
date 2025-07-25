from pydantic import BaseModel

from exercise.plugins.models import SpecType
from exercise.plugins.transformations.csv_transformation.models import (
    TransformationCSVSpec,
)

LOG_RETENTION: str = "30 days"
SPARK_CONFIG_PATH: str = "src/exercise/specs/spark_configs.yaml"
SPECS_PATH: str = "src/exercise/specs/"

SPEC_MODELS: dict[SpecType, type[BaseModel]] = {
    SpecType.TRANSFORMATION_CSV: TransformationCSVSpec,
}
