from unittest.mock import MagicMock

import pytest
from delta import configure_spark_with_delta_pip
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_fixture():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("Testing")
        .config(
            "spark.sql.shuffle.partitions", "1"
        )  # Default is 200; 1 is plenty for test data
        .config("spark.ui.enabled", "false")  # Disable web UI tracking
        .config(
            "spark.sql.execution.arrow.pyspark.enabled", "true"
        )  # Force fast Arrow serialization
        # delta stuff
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(spark).getOrCreate()
    yield spark
    spark.stop()


class FakePy4JJavaError(Py4JJavaError):
    """Concrete Py4JJavaError we can actually `raise` in tests.

    Py4JJavaError.__init__ expects a real java_exception with a .stackTrace,
    so we skip it and initialise it as a plain Exception instead.

    We still need to be safe to `str()` because PySpark's log formatter calls
    str(exc_value) on the exception record.
    """

    def __init__(self, msg: str = "fake java error"):
        Exception.__init__(self, msg)
        self._msg = msg
        # Dummy so Py4JJavaError.__str__ (if ever reached) doesn't blow up.
        self.java_exception = MagicMock()
        self.java_exception._gateway_client = MagicMock()
        self.java_exception.stackTrace = []

    def __str__(self) -> str:
        return self._msg

    def __repr__(self) -> str:
        return f"FakePy4JJavaError({self._msg!r})"
