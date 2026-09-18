from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest
from chispa import assert_df_equality
from conftest import FakePy4JJavaError
from py4j.protocol import Py4JJavaError
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException, ParseException

from iot_pipeline.config import QualityConfig, _Config
from iot_pipeline.exceptions import DataQualityException, ZeroRowsDataFrameException
from iot_pipeline.quality import (
    backup_rescued_data,
    calculate_clean,
    enforce_quality_rules,
    validate_milli_timestamp,
    validate_positive_integer,
    validate_three_char_str,
    validate_two_char_str,
)

# testing happy validations


@pytest.mark.integration
def test_validate_three_char_str(spark_fixture):
    columns = ["cca3"]

    input_data = [
        {"cca2": "US", "cca3": "USA", "c02_level": 868, "timestamp": 1458444054093},
        {"cca2": "NO", "cca3": "NOR", "c02_level": 1473, "timestamp": 1458444054119},
        {"cca2": "IT", "cca3": "ITALY", "c02_level": 1556, "timestamp": 1458444054120},
    ]

    output_data = [
        {"cca2": "IT", "cca3": "ITALY", "c02_level": 1556, "timestamp": 1458444054120},
    ]

    schema = StructType(
        [
            StructField("cca2", StringType(), True),
            StructField("cca3", StringType(), True),
            StructField("c02_level", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    input_df = spark_fixture.createDataFrame(input_data, schema=schema)
    expected_df = spark_fixture.createDataFrame(output_data, schema=schema)

    actual_df = validate_three_char_str(columns, input_df, schema.toDDL())

    assert_df_equality(actual_df, expected_df)


@pytest.mark.integration
def test_validate_two_char_str(spark_fixture):
    columns = ["cca2"]

    input_data = [
        {"cca2": "US", "cca3": "USA", "c02_level": 868, "timestamp": 1458444054093},
        {"cca2": "NO", "cca3": "NOR", "c02_level": 1473, "timestamp": 1458444054119},
        {"cca2": "ITALY", "cca3": "USA", "c02_level": 1556, "timestamp": 1458444054120},
    ]

    output_data = [
        {"cca2": "ITALY", "cca3": "USA", "c02_level": 1556, "timestamp": 1458444054120},
    ]

    schema = StructType(
        [
            StructField("cca2", StringType(), True),
            StructField("cca3", StringType(), True),
            StructField("c02_level", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    input_df = spark_fixture.createDataFrame(input_data, schema=schema)
    expected_df = spark_fixture.createDataFrame(output_data, schema=schema)

    actual_df = validate_two_char_str(columns, input_df, schema.toDDL())

    assert_df_equality(actual_df, expected_df)


@pytest.mark.integration
def test_validate_positive_integer(spark_fixture):
    columns = ["c02_level"]

    input_data = [
        {"cca2": "US", "cca3": "USA", "c02_level": 868, "timestamp": 1458444054093},
        {"cca2": "NO", "cca3": "NOR", "c02_level": 1473, "timestamp": 1458444054119},
        {"cca2": "IT", "cca3": "ITA", "c02_level": -1, "timestamp": 1458444054120},
    ]

    output_data = [
        {"cca2": "IT", "cca3": "ITA", "c02_level": -1, "timestamp": 1458444054120},
    ]

    schema = StructType(
        [
            StructField("cca2", StringType(), True),
            StructField("cca3", StringType(), True),
            StructField("c02_level", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    input_df = spark_fixture.createDataFrame(input_data, schema=schema)
    expected_df = spark_fixture.createDataFrame(output_data, schema=schema)

    actual_df = validate_positive_integer(columns, input_df, schema.toDDL())

    assert_df_equality(actual_df, expected_df)


@pytest.mark.integration
def test_validate_milli_timestamp(spark_fixture):
    columns = ["timestamp"]

    input_data = [
        {"cca2": "US", "cca3": "USA", "c02_level": 868, "timestamp": 1458444054093},
        {"cca2": "NO", "cca3": "NOR", "c02_level": 1473, "timestamp": 1458444054119},
        {"cca2": "IT", "cca3": "ITA", "c02_level": 1556, "timestamp": 10},
    ]

    output_data = [
        {"cca2": "IT", "cca3": "ITA", "c02_level": 1556, "timestamp": 10},
    ]

    schema = StructType(
        [
            StructField("cca2", StringType(), True),
            StructField("cca3", StringType(), True),
            StructField("c02_level", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    input_df = spark_fixture.createDataFrame(input_data, schema=schema)
    expected_df = spark_fixture.createDataFrame(output_data, schema=schema)

    actual_df = validate_milli_timestamp(columns, input_df, schema.toDDL())

    assert_df_equality(actual_df, expected_df)


@pytest.mark.integration
def test_calculate_clean_happy_path(spark_fixture):

    raw_data = [
        {"cca2": "US", "cca3": "USA", "c02_level": 868, "timestamp": 1458444054093},
        {"cca2": "NO", "cca3": "NOR", "c02_level": 1473, "timestamp": 1458444054119},
        {"cca2": "IT", "cca3": "ITA", "c02_level": 1556, "timestamp": 14584440541190},
        {"cca2": "IT", "cca3": "ITA", "c02_level": 1556, "timestamp": 14584440541190},
    ]

    clean_data = [
        {"cca2": "US", "cca3": "USA", "c02_level": 868, "timestamp": 1458444054093},
        {"cca2": "NO", "cca3": "NOR", "c02_level": 1473, "timestamp": 1458444054119},
    ]

    schema = StructType(
        [
            StructField("cca2", StringType(), True),
            StructField("cca3", StringType(), True),
            StructField("c02_level", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    df = spark_fixture.createDataFrame(raw_data, schema=schema)
    clean_df = spark_fixture.createDataFrame(clean_data, schema=schema)
    assert calculate_clean(df, clean_df) == 50


@pytest.mark.integration
def test_calculate_clean_exception(spark_fixture):
    schema = StructType(
        [
            StructField("cca2", StringType(), True),
            StructField("cca3", StringType(), True),
            StructField("c02_level", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    df = spark_fixture.emptyDataFrame(schema)
    with pytest.raises(ZeroRowsDataFrameException):
        calculate_clean(df, df)


@pytest.mark.integration
def test_backup_rescued_data_happy_path(spark_fixture):
    raw_data = [
        {"cca2": "US", "cca3": "USA", "c02_level": 868, "timestamp": 1458444054093},
        {"cca2": "NO", "cca3": "NOR", "c02_level": 1473, "timestamp": 1458444054119},
        {"cca2": "IT", "cca3": "ITA", "c02_level": 1556, "timestamp": 14584440541190},
        {"cca2": "IT", "cca3": "ITA", "c02_level": 1556, "timestamp": 14584440541190},
    ]
    schema = StructType(
        [
            StructField("cca2", StringType(), True),
            StructField("cca3", StringType(), True),
            StructField("c02_level", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    df = spark_fixture.createDataFrame(raw_data, schema)
    with TemporaryDirectory(dir="tests") as tmp:
        backup_rescued_data(df, tmp, "col")
        assert (Path(tmp) / "col.csv").exists()
        actual = spark_fixture.read.csv(
            str(Path(tmp) / "col.csv"), header=True, schema=schema
        )
        assert_df_equality(actual, df, ignore_nullable=True)


@pytest.mark.integration
def test_backup_rescued_data_analysis_parse_exception(spark_fixture):
    raw_data = [
        {"cca2": "US", "cca3": "USA", "c02_level": 868, "timestamp": 1458444054093},
        {"cca2": "NO", "cca3": "NOR", "c02_level": 1473, "timestamp": 1458444054119},
        {"cca2": "IT", "cca3": "ITA", "c02_level": 1556, "timestamp": 14584440541190},
        {"cca2": "IT", "cca3": "ITA", "c02_level": 1556, "timestamp": 14584440541190},
    ]
    schema = StructType(
        [
            StructField("cca2", StringType(), True),
            StructField("cca3", StringType(), True),
            StructField("c02_level", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    df = spark_fixture.createDataFrame(raw_data, schema)
    with pytest.raises((AnalysisException, ParseException)):
        input = df.select("None_existing_col")
        backup_rescued_data(input, "", "")


def test_backup_rescued_data_java_error():

    mock_writer = MagicMock()
    mock_writer.csv.side_effect = FakePy4JJavaError
    mock_df = MagicMock()
    mock_df.write = mock_writer

    with pytest.raises(Py4JJavaError):
        backup_rescued_data(mock_df, "", "")


@pytest.mark.integration
def test_enforce_quality_rules_data_quality_exception(spark_fixture):
    config = QualityConfig(
        _Config(
            {
                "EXTRACT_SCHEMA": "cca2 STRING, cca3 STRING, c02_level INT, timestamp LONG",
                "DIRTY_DATA_THRESHOLD_PCT": -10,
            }
        )
    )
    raw_data = [
        {"cca2": "US", "cca3": "USA", "c02_level": 868, "timestamp": 1458444054093},
        {"cca2": "NO", "cca3": "NOR", "c02_level": 1473, "timestamp": 1458444054119},
        {"cca2": "IT", "cca3": "ITA", "c02_level": 1556, "timestamp": 14584440541190},
        {"cca2": "IT", "cca3": "ITA", "c02_level": 1556, "timestamp": 14584440541190},
    ]
    schema = StructType(
        [
            StructField("cca2", StringType(), True),
            StructField("cca3", StringType(), True),
            StructField("c02_level", IntegerType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    df = spark_fixture.createDataFrame(raw_data, schema)
    with pytest.raises(DataQualityException):
        enforce_quality_rules(df, config)
