import os
import sys
from pathlib import Path

from iot_pipeline.config import SessionConfig, get_config
from iot_pipeline.logs import get_logger, setup_logging
from iot_pipeline.pipeline import run_pipeline
from iot_pipeline.session import get_session

logger = get_logger(__name__)


def main():
    if len(sys.argv) < 2:
        raise RuntimeError("Usage: script <path-to-configuration-yaml>")
    elif not Path(sys.argv[1]).exists() or Path(sys.argv[1]).suffix != ".yaml":
        raise RuntimeError("You need to pass an existing '.yaml' file as configuration")

    config = get_config(sys.argv[1])
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        f"--driver-memory {config.SPARK_DRIVER_MEMORY} pyspark-shell"
    )

    setup_logging(config.LOGLEVEL, config.LOGFILE)

    spark = get_session(SessionConfig(config))
    spark = run_pipeline(spark, config)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
