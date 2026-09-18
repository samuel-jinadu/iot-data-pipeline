from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException, ParseException

from iot_pipeline.config import QualityConfig
from iot_pipeline.exceptions import DataQualityException, ZeroRowsDataFrameException
from iot_pipeline.logs import get_logger

logger = get_logger(__name__)


def enforce_quality_rules(df: DataFrame, quality_config: QualityConfig):
    logger.info("Running validations")

    rescued_records_df = df.sparkSession.emptyDataFrame(quality_config.EXTRACT_SCHEMA)
    if quality_config.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD in df.columns:
        rescued_records_df = df.filter(
            col(quality_config.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD).isNotNull()
        )
    rescued_records_df = rescued_records_df.union(
        validate_three_char_str(
            quality_config.THREE_CHAR_STR, df, quality_config.EXTRACT_SCHEMA
        )
    )
    rescued_records_df = rescued_records_df.union(
        validate_two_char_str(
            quality_config.TWO_CHAR_STR, df, quality_config.EXTRACT_SCHEMA
        )
    )
    rescued_records_df = rescued_records_df.union(
        validate_positive_integer(
            quality_config.POSITIVE_INTEGER, df, quality_config.EXTRACT_SCHEMA
        )
    )
    rescued_records_df = rescued_records_df.union(
        validate_milli_timestamp(
            quality_config.MILLI_TIMESTAMP, df, quality_config.EXTRACT_SCHEMA
        )
    )

    clean_df = df.subtract(rescued_records_df)
    rescued_records_df = rescued_records_df.distinct()
    clean_df = clean_df.drop(quality_config.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD)

    try:
        pct_clean = calculate_clean(df, clean_df)
    except ZeroRowsDataFrameException:
        logger.exception("Empty bronze data!")
        raise
    if pct_clean < (100 - quality_config.DIRTY_DATA_THRESHOLD_PCT):
        try:
            backup_rescued_data(
                rescued_records_df,
                quality_config.OUTPUTFOLDERPATH,
                quality_config.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD,
            )
            raise DataQualityException(
                f"There are too many bad rows - {100 - pct_clean}% of rows are dirty, and the threshold is {quality_config.DIRTY_DATA_THRESHOLD_PCT}%"
            )
        except DataQualityException:
            logger.exception("")
            raise
        except Exception:
            raise

    return clean_df


def calculate_clean(df: DataFrame, clean_df: DataFrame) -> float:
    total = df.count()
    if total == 0:
        raise ZeroRowsDataFrameException("There's no rows in the raw DataFrame")
    return (clean_df.count() / total) * 100


def backup_rescued_data(
    df: DataFrame, OUTPUTFOLDERPATH: str, SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD: str
):
    try:
        logger.info("Backing up rescued data")
        path = Path(OUTPUTFOLDERPATH) / (
            SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD + ".csv"
        )
        df.write.csv(str(path), mode="overwrite", header=True)
    except (AnalysisException, ParseException):
        logger.exception(
            "There is an issue with the transformations associated with the rescued data, check the query plan, column references and schema"
        )
        raise
    except Py4JJavaError:
        logger.exception(
            "There was a java error when trying to write rescued data to disk"
        )
        raise
    except Exception:
        logger.exception(
            "An unexpected error occurred while saving the rescued data to disk"
        )
        raise


def validate_three_char_str(columns: list[str], df: DataFrame, EXTRACT_SCHEMA: str):
    invalid_records_df = df.sparkSession.emptyDataFrame(EXTRACT_SCHEMA)
    for columnName in columns:
        invalid_records_df = invalid_records_df.union(
            df.where(col(columnName).isNull() | (char_length(columnName) != lit(3)))
        )
    return invalid_records_df


def validate_two_char_str(columns: list[str], df: DataFrame, EXTRACT_SCHEMA: str):
    invalid_records_df = df.sparkSession.emptyDataFrame(EXTRACT_SCHEMA)
    for columnName in columns:
        invalid_records_df = invalid_records_df.union(
            df.where(col(columnName).isNull() | (char_length(columnName) != lit(2)))
        )
    return invalid_records_df


def validate_positive_integer(columns: list[str], df: DataFrame, EXTRACT_SCHEMA: str):
    invalid_records_df = df.sparkSession.emptyDataFrame(EXTRACT_SCHEMA)
    for columnName in columns:
        invalid_records_df = invalid_records_df.union(
            df.where(col(columnName).isNull() | (col(columnName) < 0))
        )
    return invalid_records_df


def validate_milli_timestamp(columns: list[str], df: DataFrame, EXTRACT_SCHEMA: str):
    invalid_records_df = df.sparkSession.emptyDataFrame(EXTRACT_SCHEMA)
    for columnName in columns:
        # the true birth of commercial internet-connected devices is pinned to 1990 i.e. 631152000000 source: https://www.fia.uk.com/news/history-of-iot.html
        invalid_records_df = invalid_records_df.union(
            df.where(
                col(columnName).isNull()
                | (col(columnName) < 631152000000)
                | (col(columnName) > 10000000000000)
            )
        )
    return invalid_records_df
