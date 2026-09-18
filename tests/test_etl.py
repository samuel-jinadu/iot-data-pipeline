from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock

import pytest
from chispa import assert_df_equality
from conftest import FakePy4JJavaError
from py4j.protocol import Py4JJavaError
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException, ParseException

from iot_pipeline.config import CleanConfig, _Config
from iot_pipeline.etl import clean, extract, save_bronze, save_gold, save_silver


@pytest.mark.integration
def test_extract_happy_path(spark_fixture):

    data = [
        {
            "device_id": 1,
            "device_name": "meter-gauge-1xbYRYcj",
            "ip": "68.161.225.1",
            "cca2": "US",
            "cca3": "USA",
            "cn": "United States",
            "latitude": Decimal("38.000000"),
            "longitude": Decimal("-97.000000"),
            "scale": "Celsius",
            "temp": 34,
            "humidity": 51,
            "battery_level": 8,
            "c02_level": 868,
            "lcd": "green",
            "timestamp": 1458444054093,
        },
        {
            "device_id": 2,
            "device_name": "sensor-pad-2n2Pea",
            "ip": "213.161.254.1",
            "cca2": "NO",
            "cca3": "NOR",
            "cn": "Norway",
            "latitude": Decimal("62.470000"),
            "longitude": Decimal("6.150000"),
            "scale": "Celsius",
            "temp": 11,
            "humidity": 70,
            "battery_level": 7,
            "c02_level": 1473,
            "lcd": "red",
            "timestamp": 1458444054119,
        },
        {
            "device_id": 3,
            "device_name": "device-mac-36TWSKiT",
            "ip": "88.36.5.1",
            "cca2": "IT",
            "cca3": "ITA",
            "cn": "Italy",
            "latitude": Decimal("42.830000"),
            "longitude": Decimal("12.830000"),
            "scale": "Celsius",
            "temp": 19,
            "humidity": 44,
            "battery_level": 2,
            "c02_level": 1556,
            "lcd": "red",
            "timestamp": 1458444054120,
        },
    ]

    expected_schema = StructType(
        [
            StructField("device_id", IntegerType(), True),
            StructField("device_name", StringType(), True),
            StructField("ip", StringType(), True),
            StructField("cca2", StringType(), True),
            StructField("cca3", StringType(), True),
            StructField("cn", StringType(), True),
            StructField("latitude", DecimalType(7, 3), True),
            StructField("longitude", DecimalType(7, 3), True),
            StructField("scale", StringType(), True),
            StructField("temp", IntegerType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("battery_level", IntegerType(), True),
            StructField("c02_level", IntegerType(), True),
            StructField("lcd", StringType(), True),
            StructField("timestamp", LongType(), True),
        ]
    )

    expected_df = spark_fixture.createDataFrame(data, schema=expected_schema)

    input_schema = "device_id INT, device_name STRING, ip STRING, cca2 STRING, cca3 STRING, cn STRING, latitude DECIMAL(7,3), longitude DECIMAL(7,3), scale STRING, temp INT, humidity INT, battery_level INT, c02_level INT, lcd STRING, timestamp LONG"

    actual_df = extract(spark_fixture, "tests/fixtures/test-happy.json", input_schema)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


@pytest.mark.integration
def test_extract_analysis_exception(spark_fixture):
    input_schema = "xxx"

    with pytest.raises(AnalysisException):
        extract(spark_fixture, "tests/fixtures/test-happy.json", input_schema)


def test_extract_java_error():

    mock_reader = MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.schema.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.load.side_effect = FakePy4JJavaError

    mock_spark = MagicMock()
    mock_spark.read = mock_reader

    with pytest.raises(Py4JJavaError):
        extract(mock_spark, "/tmp/data.json", "id INT")


@pytest.mark.integration
def test_clean(spark_fixture):
    data = [("Something", "AnotherThing"), ("Something", None)]
    input = spark_fixture.createDataFrame(data, ["col1", "col2"])
    actual = clean(
        input, CleanConfig(_Config({"EXTRACT_SCHEMA": "", "DROP_ON_NULL": ["col2"]}))
    )
    expected = spark_fixture.createDataFrame(
        [("Something", "AnotherThing")], ["col1", "col2"]
    )
    assert_df_equality(actual, expected)


# writing to disk tests


@pytest.mark.integration
def test_save_bronze_happy_path(spark_fixture):
    data = [("Something", "AnotherThing"), ("Something", "YestAnotherThing")]
    input = spark_fixture.createDataFrame(data, ["col1", "col2"])

    with TemporaryDirectory(dir="tests") as tmpdir:
        save_bronze(input, "happy_bronze_test.json", tmpdir)
        actual = spark_fixture.read.format("delta").load(
            str(Path(tmpdir) / "bronze" / "happy_bronze_test.delta")
        )
        assert_df_equality(actual, input)


@pytest.mark.integration
def test_save_silver_happy_path(spark_fixture):
    data = [("Something", "AnotherThing"), ("Something", "YestAnotherThing")]
    input = spark_fixture.createDataFrame(data, ["col1", "col2"])

    with TemporaryDirectory(dir="tests") as tmpdir:
        save_silver(input, "happy_silver_test.json", tmpdir)
        actual = spark_fixture.read.format("delta").load(
            str(Path(tmpdir) / "silver" / "happy_silver_test.delta")
        )
        assert_df_equality(actual, input)


@pytest.mark.integration
def test_save_gold_happy_path(spark_fixture):
    data = [("Something", "AnotherThing"), ("Something", "YestAnotherThing")]
    input = spark_fixture.createDataFrame(data, ["col1", "col2"])

    with TemporaryDirectory(dir="tests") as tmpdir:
        save_gold(input, input, input, tmpdir, "happy_gold_test")
        actual = spark_fixture.read.format("delta").load(
            str(Path(tmpdir) / "gold" / "happy_gold_test.delta")
        )
        assert_df_equality(actual, input)


# analysis or parse exception paths


@pytest.mark.integration
def test_save_bronze_analysis_parse_exception(spark_fixture):
    data = [("Something", "AnotherThing"), ("Something", "YestAnotherThing")]
    input = spark_fixture.createDataFrame(data, ["col1", "col2"])

    with pytest.raises((AnalysisException, ParseException)):
        input = input.select("None_existing_col")
        save_bronze(input, "in", "out")


@pytest.mark.integration
def test_save_silver_analysis_parse_exception(spark_fixture):
    data = [
        ("Something", "AnotherThing"),
        ("Something", "YestAnotherThing"),
    ]
    input_df = spark_fixture.createDataFrame(data, ["col1", "col2"])

    with pytest.raises((AnalysisException, ParseException)):
        bad_df = input_df.select("None_existing_col")
        save_silver(bad_df, "in", "out")


@pytest.mark.integration
def test_save_gold_analysis_parse_exception(spark_fixture):
    data = [
        ("Something", "AnotherThing"),
        ("Something", "YestAnotherThing"),
    ]
    analysis_df = spark_fixture.createDataFrame(data, ["col1", "col2"])
    raw_df = spark_fixture.createDataFrame(data, ["col1", "col2"])
    quality_df = spark_fixture.createDataFrame(data, ["col1", "col2"])

    with pytest.raises((AnalysisException, ParseException)):
        bad_df = analysis_df.select("None_existing_col")
        save_gold(bad_df, raw_df, quality_df, "out", "my_analysis")


# java errors
def test_save_bronze_java_error():

    mock_writer = MagicMock()
    mock_writer.format.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.save.side_effect = FakePy4JJavaError

    mock_df = MagicMock()
    mock_df.write = mock_writer
    mock_df.count.return_value = 1

    with pytest.raises(Py4JJavaError):
        save_bronze(mock_df, "", "")


def test_save_silver_java_error():

    mock_writer = MagicMock()
    mock_writer.format.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.save.side_effect = FakePy4JJavaError

    mock_df = MagicMock()
    mock_df.write = mock_writer
    mock_df.count.return_value = 1

    with pytest.raises(Py4JJavaError):
        save_silver(mock_df, "", "")


def test_save_gold_java_error():

    mock_writer = MagicMock()
    mock_writer.format.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.save.side_effect = FakePy4JJavaError

    mock_df = MagicMock()
    mock_df.write = mock_writer
    mock_df.count.return_value = 1

    with pytest.raises(Py4JJavaError):
        save_gold(mock_df, mock_df, mock_df, "", "")
