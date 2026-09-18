class ConfigurationValidationException(RuntimeError):
    """Raised when a configuration value is invalid."""


class DataQualityException(RuntimeError):
    """Raised when there is a data quality issue, like too many nulls"""


class ConfigurationImmutabilityRule(RuntimeError):
    """Raised when someone attempts to modify the the attributes of an instance of a config class"""


# this one isnt an exception, but i was already calling this file exceptions by the time i realised i needed it, so...


class ConfigurationValidationWarning(UserWarning):
    """Raised when someone attempts to use a valid configuration that is sus"""


class MissingConfigurationKeyNoReasonableDefaultException(RuntimeError):
    """Raised when there is a missing key in the config yaml that doesnt have a reasonable default. Something like EXTRACT_SCHEMA"""


class ZeroRowsDataFrameException(RuntimeError):
    """Raised when the count method of a DataFrame returns zero"""
