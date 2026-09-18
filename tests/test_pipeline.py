from tempfile import TemporaryDirectory

import pytest

from iot_pipeline.config import _Config
from iot_pipeline.pipeline import run_pipeline


@pytest.mark.integration
def test_pipeline_happy_path(spark_fixture):
    with TemporaryDirectory(dir="tests") as tmpdir:
        config = _Config(
            {
                "INPUTDATAPATH": "tests/fixtures/test-happy.json",
                "OUTPUTFOLDERPATH": tmpdir,
                "EXTRACT_SCHEMA": "device_id INT, device_name STRING, ip STRING, cca2 STRING, cca3 STRING, cn STRING, latitude DECIMAL(7,3), longitude DECIMAL(7,3), scale STRING, temp INT, humidity INT, battery_level INT, c02_level INT, lcd STRING, timestamp LONG",
            }
        )
        assert run_pipeline(spark_fixture, config) == spark_fixture
