from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from iot_pipeline.config import SessionConfig
from iot_pipeline.logs import get_logger

logger = get_logger(__name__)


def get_session(session_config: SessionConfig):
    logger.info("Building spark session...")
    spark = (
        SparkSession.builder.master(session_config.SPARK_MASTER)
        .appName(session_config.APPNAME)
        .config(
            "spark.sql.shuffle.partitions", session_config.SPARK_SQL_SHUFFLE_PARTITIONS
        )
        .config("spark.sql.session.timeZone", session_config.SPARK_SQL_SESSION_TIMEZONE)
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            session_config.SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE,
        )
        .config("spark.serializer", session_config.SPARK_SERIALIZER)
        .config(
            "spark.sql.parquet.compression.codec",
            session_config.SPARK_SQL_PARQUET_COMPRESSION_CODEC,
        )
        .config(
            "spark.sql.columnNameOfCorruptRecord",
            session_config.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD,
        )
        .config("spark.memory.fraction", session_config.SPARK_MEMORY_FRACTION)
        # these two aren't really optional and they are required for using delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(spark).getOrCreate()
