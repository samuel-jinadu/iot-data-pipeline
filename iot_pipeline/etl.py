from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException, ParseException

from iot_pipeline.config import CleanConfig
from iot_pipeline.logs import get_logger

logger = get_logger(__name__)


def extract(spark: SparkSession, INPUTDATAPATH: str, EXTRACT_SCHEMA: str) -> DataFrame:
    logger.info(f"Extracting '{INPUTDATAPATH}'")
    ext = Path(INPUTDATAPATH).suffix
    try:
        if ext == ".json" or ext == ".jsonl":
            return (
                spark.read.format("json")
                .schema(EXTRACT_SCHEMA)
                .option("mode", "PERMISSIVE")
                .load(INPUTDATAPATH)
            )
        elif ext == ".csv":
            return (
                spark.read.format("csv")
                .schema(EXTRACT_SCHEMA)
                .option("mode", "PERMISSIVE")
                .option("header", True)
                .load(INPUTDATAPATH)
            )
    except AnalysisException:
        logger.exception(f"There was an issue parsing the DDL string: {EXTRACT_SCHEMA}")
        raise
    except Py4JJavaError:
        logger.exception(
            f"JVM-side exception has occurred when trying to load: {INPUTDATAPATH}"
        )
        raise
    except Exception:
        logger.error(
            f"An unknown error has occured while extracting {INPUTDATAPATH} with {EXTRACT_SCHEMA}"
        )
        raise


def clean(df: DataFrame, clean_config: CleanConfig) -> DataFrame:
    logger.info("Cleaning data")
    clean_df = df.na.drop(subset=clean_config.DROP_ON_NULL)
    return clean_df


# writing to disk


def save_bronze(df: DataFrame, INPUTDATAPATH: str, OUTPUTFOLDERPATH: str):
    path = Path(OUTPUTFOLDERPATH) / "bronze" / (Path(INPUTDATAPATH).stem + ".delta")
    try:
        logger.info(f"Saving {df.count()} bronze rows to disk")
        df.write.format("delta").mode("overwrite").save(str(path))
    except (AnalysisException, ParseException):
        logger.exception(
            "There is an issue with the transformations associated with the bronze data, check the query plan, column references and schema"
        )
        raise
    except Py4JJavaError:
        logger.exception(
            "There was a java error when trying to write bronze data to disk"
        )
        raise
    except Exception:
        logger.exception(
            "An unexpected error occurred while saving the bronze data to disk"
        )
        raise


def save_silver(df: DataFrame, INPUTDATAPATH: str, OUTPUTFOLDERPATH: str):
    path = Path(OUTPUTFOLDERPATH) / "silver" / (Path(INPUTDATAPATH).stem + ".delta")
    try:
        logger.info(f"Saving {df.count()} silver rows to disk")
        df.write.format("delta").mode("overwrite").save(str(path))
    except (AnalysisException, ParseException):
        logger.exception(
            "There is an issue with the transformations associated with the silver data, check the query plan, column references and schema"
        )
        raise
    except Py4JJavaError:
        logger.exception(
            "There was a java error when trying to write silver data to disk"
        )
        raise
    except Exception:
        logger.exception(
            "An unexpected error occurred while saving the silver data to disk"
        )
        raise


def save_gold(
    analysis_df: DataFrame,
    raw_df: DataFrame,
    quality_df: DataFrame,
    OUTPUTFOLDERPATH: str,
    analysis: str,
) -> None:
    try:
        logger.info(f"Loaded {raw_df.count()} rows from disk")
        logger.info(f"Kept {quality_df.count()} rows after cleaning")
        logger.info(f"Saving {analysis_df.count()} gold rows to disk")
        path = Path(OUTPUTFOLDERPATH) / "gold" / (analysis + ".delta")
        analysis_df.write.format("delta").mode("overwrite").save(str(path))
    except (AnalysisException, ParseException):
        logger.exception(
            "There is an issue with the transformations associated with the gold data, check the query plan, column references and schema"
        )
        raise
    except Py4JJavaError:
        logger.exception(
            "There was a java error when trying to write gold data to disk"
        )
        raise
    except Exception:
        logger.exception(
            "An unexpected error occurred while saving the gold data to disk"
        )
        raise
