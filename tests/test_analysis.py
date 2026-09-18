import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession

from iot_pipeline.analysis import devices_per_country


@pytest.mark.integration
def test_devices_per_country(spark_fixture: SparkSession):
    input = [("United States",), ("United States",), ("United States",)]
    output = [("United States", 3)]
    expected = spark_fixture.createDataFrame(output, ["cn", "total_number_of_devices"])
    input = spark_fixture.createDataFrame(input, ["cn"])
    actual = devices_per_country(input)
    assert_df_equality(actual, expected, ignore_nullable=True)
