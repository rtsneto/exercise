"""
Spark UDFs used in the extractions and transformations
"""

import pandas as pd
import requests
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType


@pandas_udf(StringType())
def generate_nse_id(series: pd.Series) -> pd.Series:
    """Generate NSE ID using an external API call"""

    def get_hashify_value(value):
        if value is None:
            return None
        try:
            url = f"https://api.hashify.net/hash/md4/hex?value={value}"

            r = requests.get(url, timeout=3)
            r.raise_for_status()
            return r.json().get("Digest")
        except Exception:
            return None

    # Apply the API call to each element in the Series
    return series.apply(get_hashify_value)
