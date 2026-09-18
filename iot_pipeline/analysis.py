from pyspark.sql import DataFrame
from pyspark.sql.functions import *

from iot_pipeline.logs import get_logger

logger = get_logger(__name__)


def devices_per_country(df: DataFrame) -> DataFrame:
    return df.groupBy(col("cn")).agg(count("*").alias("total_number_of_devices"))
