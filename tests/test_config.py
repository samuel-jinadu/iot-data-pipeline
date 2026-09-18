import pytest

from iot_pipeline.config import get_config
from iot_pipeline.exceptions import (
    ConfigurationValidationException,
    MissingConfigurationKeyNoReasonableDefaultException,
)


# happy path
def test_get_config():
    config = get_config("tests/fixtures/test-happy.yaml")
    assert config.APPNAME == "Test"


# sad path
def test_get_config_failure_missing_key():
    with pytest.raises(MissingConfigurationKeyNoReasonableDefaultException):
        get_config("tests/fixtures/test-sad.yaml")


def test_get_config_failure_invalid_config_value():
    with pytest.raises(ConfigurationValidationException):
        get_config("tests/fixtures/test-nonesense.yaml")
