from pyspark.sql import SparkSession

from iot_pipeline.analysis import devices_per_country
from iot_pipeline.config import (
    CleanConfig,
    QualityConfig,
    _Config,
)
from iot_pipeline.etl import clean, extract, save_bronze, save_gold, save_silver
from iot_pipeline.logs import get_logger
from iot_pipeline.quality import enforce_quality_rules

logger = get_logger(__name__)


def run_pipeline(spark: SparkSession, config: _Config):

    # ET of the ETL
    df = extract(spark, config.INPUTDATAPATH, config.EXTRACT_SCHEMA).cache()

    save_bronze(df, config.INPUTDATAPATH, config.OUTPUTFOLDERPATH)

    quality_df = enforce_quality_rules(df, QualityConfig(config))
    quality_df = clean(quality_df, CleanConfig(config)).cache()
    save_silver(quality_df, config.INPUTDATAPATH, config.OUTPUTFOLDERPATH)

    # Analysis and Saving to disk
    analysis_df = devices_per_country(quality_df)
    if not config.CHECK_EXECUTION_PLAN:
        save_gold(
            analysis_df,
            raw_df=df,
            quality_df=quality_df,
            OUTPUTFOLDERPATH=config.OUTPUTFOLDERPATH,
            analysis="devices_per_country",
        )
    else:
        analysis_df.explain()

    return spark
