import warnings
from pathlib import Path

import yaml
from iot_pipeline.exceptions import (
    ConfigurationImmutabilityRule,
    ConfigurationValidationException,
    ConfigurationValidationWarning,
    MissingConfigurationKeyNoReasonableDefaultException,
)


class _FrozenConfig:
    def __setattr__(self, name, value):
        if getattr(self, "_frozen", False):
            raise ConfigurationImmutabilityRule(
                f"You are not allowed to edit instance attributes of a config class - {name} is read-only after init"
            )
        super().__setattr__(name, value)


class _Config(_FrozenConfig):
    def __init__(self, _config):
        self.LOGLEVEL = _config.get("LOGLEVEL", "WARNING")
        self.LOGFILE = _config.get("LOGFILE", "logs/pipeline.log")
        self.APPNAME = _config.get("APPNAME", "IoT Pipeline")
        self.INPUTDATAPATH = _config.get("INPUTDATAPATH", "data/iot_devices.json")
        self.OUTPUTFOLDERPATH = _config.get("OUTPUTFOLDERPATH", "data/output")
        self.EXTRACT_SCHEMA = _config["EXTRACT_SCHEMA"]  # required, no sane default
        self.SPARK_MASTER = _config.get("SPARK_MASTER", "local[*]")
        self.SPARK_SQL_SHUFFLE_PARTITIONS = _config.get(
            "SPARK_SQL_SHUFFLE_PARTITIONS", 8
        )
        self.SPARK_SQL_SESSION_TIMEZONE = _config.get(
            "SPARK_SQL_SESSION_TIMEZONE", "UTC"
        )
        self.SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE = _config.get(
            "SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE", "dynamic"
        )
        self.SPARK_SERIALIZER = _config.get(
            "SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer"
        )
        self.SPARK_SQL_PARQUET_COMPRESSION_CODEC = _config.get(
            "SPARK_SQL_PARQUET_COMPRESSION_CODEC", "zstd"
        )
        self.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD = _config.get(
            "SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD", "_rescued_data"
        )
        self.DIRTY_DATA_THRESHOLD_PCT = _config.get("DIRTY_DATA_THRESHOLD_PCT", 10)
        self.THREE_CHAR_STR = _config.get("THREE_CHAR_STR", [])
        self.TWO_CHAR_STR = _config.get("TWO_CHAR_STR", [])
        self.POSITIVE_INTEGER = _config.get(
            "POSITIVE_INTEGER",
            [],
        )
        self.MILLI_TIMESTAMP = _config.get("MILLI_TIMESTAMP", [])
        self.CHECK_EXECUTION_PLAN = _config.get("CHECK_EXECUTION_PLAN", False)
        self.DROP_ON_NULL = _config.get("DROP_ON_NULL", [])
        self.SPARK_MEMORY_FRACTION = _config.get("SPARK_MEMORY_FRACTION", 0.6)
        self.SPARK_DRIVER_MEMORY = _config.get("SPARK_DRIVER_MEMORY", "1g")

        self._frozen = True


# Bloated function signatures config classes


class SessionConfig(_FrozenConfig):
    def __init__(self, config: _Config):
        self.APPNAME = config.APPNAME
        self.SPARK_MASTER = config.SPARK_MASTER
        self.SPARK_SQL_SHUFFLE_PARTITIONS = config.SPARK_SQL_SHUFFLE_PARTITIONS
        self.SPARK_SQL_SESSION_TIMEZONE = config.SPARK_SQL_SESSION_TIMEZONE
        self.SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE = (
            config.SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE
        )
        self.SPARK_SERIALIZER = config.SPARK_SERIALIZER
        self.SPARK_SQL_PARQUET_COMPRESSION_CODEC = (
            config.SPARK_SQL_PARQUET_COMPRESSION_CODEC
        )
        self.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD = (
            config.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD
        )
        self.SPARK_MEMORY_FRACTION = config.SPARK_MEMORY_FRACTION

        self._frozen = True


class CleanConfig(_FrozenConfig):
    def __init__(self, config: _Config):
        self.DROP_ON_NULL = config.DROP_ON_NULL
        self._frozen = True


class QualityConfig(_FrozenConfig):
    def __init__(self, config: _Config):
        self.DIRTY_DATA_THRESHOLD_PCT = config.DIRTY_DATA_THRESHOLD_PCT
        self.THREE_CHAR_STR = config.THREE_CHAR_STR
        self.TWO_CHAR_STR = config.TWO_CHAR_STR
        self.POSITIVE_INTEGER = config.POSITIVE_INTEGER
        self.MILLI_TIMESTAMP = config.MILLI_TIMESTAMP
        self.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD = (
            config.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD
        )
        self.EXTRACT_SCHEMA = config.EXTRACT_SCHEMA
        self.OUTPUTFOLDERPATH = config.OUTPUTFOLDERPATH

        self._frozen = True


def get_config(path: str):
    with open(path) as f:
        _config = yaml.safe_load(f)
    try:
        config = _Config(_config)
        return validate_config(config)
    except KeyError:
        raise MissingConfigurationKeyNoReasonableDefaultException(
            "Could'nt make a default for missing config key - Some culprits are EXTRACT_SCHEMA"
        )
    except ConfigurationValidationException:
        raise
    except Exception:
        raise


def validate_config(config: _Config) -> _Config:
    # must be one of the following
    if config.LOGLEVEL not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
        raise ConfigurationValidationException(
            f"'LOGLEVEL' is not set to a reasonable value - set it to either DEBUG, INFO, WARNING, ERROR, and CRITICAL - It is currently set to {config.LOGLEVEL}"
        )

    # must be a str and an existing path and a file
    if (
        (not isinstance(config.LOGFILE, str))
        or (not Path(config.LOGFILE).parent.exists())
        or (not Path(config.LOGFILE).parent.is_dir())
    ):
        raise ConfigurationValidationException(
            f"LOGFILE is either not a string, or the folder does not exist or its parent is not a folder - look at it for yourself {config.LOGFILE}"
        )

    # must exist and either be a .json/.jsonl/.csv file
    if (
        (not isinstance(config.INPUTDATAPATH, str))
        or (not Path(config.INPUTDATAPATH).exists())
        or (not Path(config.INPUTDATAPATH).is_file())
        or (not Path(config.INPUTDATAPATH).suffix in [".json", ".jsonl", ".csv"])
    ):
        raise ConfigurationValidationException(
            f"INPUTDATAPATH is either not a string, an existing path, a file or a .json/.jsonl/.csv - take a look {config.INPUTDATAPATH}"
        )

    # must be a str and an existing path and a foler
    if (
        (not isinstance(config.OUTPUTFOLDERPATH, str))
        or (not Path(config.OUTPUTFOLDERPATH).exists())
        or (not Path(config.OUTPUTFOLDERPATH).is_dir())
    ):
        raise ConfigurationValidationException(
            f"OUTPUTFOLDERPATH is either not a string, existing path or a folder: {config.OUTPUTFOLDERPATH}"
        )

    # must be a positive double and less than 100
    if (
        (not isinstance(config.DIRTY_DATA_THRESHOLD_PCT, (int, float)))
        or (config.DIRTY_DATA_THRESHOLD_PCT < 0)
        or (config.DIRTY_DATA_THRESHOLD_PCT > 100)
    ):
        raise ConfigurationValidationException(
            f"DIRTY_DATA_THRESHOLD_PCT is either not an number, is negative or is above 100: {config.DIRTY_DATA_THRESHOLD_PCT}"
        )

    # must be a list of strings but just warns if the list is empty
    business_decisions = [
        config.THREE_CHAR_STR,
        config.TWO_CHAR_STR,
        config.POSITIVE_INTEGER,
        config.MILLI_TIMESTAMP,
        config.DROP_ON_NULL,
    ]
    business_decisions_validations = [
        (not is_seq_of_str(x)) for x in business_decisions
    ]
    if True in business_decisions_validations:
        raise ConfigurationValidationException(
            "One of the business decisions config is not a sequence of strings - check THREE_CHAR_STR, TWO_CHAR_STR, POSITIVE_INTEGER, MILLI_TIMESTAMP, or DROP_ON_NULL"
        )
    business_decisions_validations = [
        isinstance(x, (list, tuple)) and (len(x) == 0) for x in business_decisions
    ]
    if True in business_decisions_validations:
        warnings.warn(
            "One of the business decisions is an empty list - check THREE_CHAR_STR, TWO_CHAR_STR, POSITIVE_INTEGER, MILLI_TIMESTAMP, or DROP_ON_NULL",
            ConfigurationValidationWarning,
        )

    # throughly testing the validity of a DDL string is beyond the scope of this project
    # the assumption is that data almost always includes some kind of string especially business data or even in this case of IoT data, you need it for device names at least or something
    # another assumption is that you have more than one column
    if (
        (not isinstance(config.EXTRACT_SCHEMA, str))
        or (not "STRING" in config.EXTRACT_SCHEMA)
        or (len(config.EXTRACT_SCHEMA.split(",")) == 1)
    ):
        raise ConfigurationValidationException(
            f"Invalid DDL string: {config.EXTRACT_SCHEMA}"
        )

    # string checks
    if (
        (not isinstance(config.APPNAME, str))
        or (not isinstance(config.SPARK_MASTER, str))
        or (not isinstance(config.SPARK_SQL_SESSION_TIMEZONE, str))
        or (not isinstance(config.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD, str))
    ):
        raise ConfigurationValidationException(
            f"APPNAME, SPARK_MASTER, or SPARK_SQL_SESSION_TIMEZONE is not a string -  SPARK_MASTER: {config.SPARK_MASTER}, APPNAME: {config.APPNAME}, SPARK_SQL_SESSION_TIMEZONE: {config.SPARK_SQL_SESSION_TIMEZONE}, SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD: {config.SPARK_SQL_COLUMN_NAME_OF_CORRUPT_RECORD}"
        )

    # a positive inteer beyween 1 and 10000
    if (config.SPARK_SQL_SHUFFLE_PARTITIONS < 1) or (
        config.SPARK_SQL_SHUFFLE_PARTITIONS > 10000
    ):
        raise ConfigurationValidationException(
            f"SPARK_SQL_SHUFFLE_PARTITIONS is an unreasoonable number: {config.SPARK_SQL_SHUFFLE_PARTITIONS}"
        )

    # is either a string with value = dynamic or static
    if (not isinstance(config.SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE, str)) or (
        config.SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE.lower()
        not in ["dynamic", "static"]
    ):
        raise ConfigurationValidationException(
            f"Value for SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE is nonesense: {config.SPARK_SQL_SOURCES_PARTITION_OVERWRITE_MODE}"
        )

    # is a member of serialisation options
    serialisation_options = [
        "org.apache.spark.serializer.DeserializationStream",
        "org.apache.spark.serializer.DummySerializerInstance",
        "org.apache.spark.serializer.JavaSerializer",
        "org.apache.spark.serializer.KryoRegistrator",
        "org.apache.spark.serializer.KryoSerializer",
        "org.apache.spark.serializer.SerializationStream",
        "org.apache.spark.serializer.Serializer",
        "org.apache.spark.serializer.SerializerInstance",
    ]
    if config.SPARK_SERIALIZER not in serialisation_options:
        raise ConfigurationValidationException(
            f"SPARK_SERIALIZER is invalid: {config.SPARK_SERIALIZER} \ncheck https://spark.apache.org/docs/latest/configuration.html "
        )

    # is a member of codec options
    codec_options = [
        "none",
        "uncompressed",
        "snappy",
        "gzip",
        "lzo",
        "brotli",
        "lz4",
        "lz4_raw",
        "zstd",
    ]
    if config.SPARK_SQL_PARQUET_COMPRESSION_CODEC not in codec_options:
        raise ConfigurationValidationException(
            f"SPARK_SQL_PARQUET_COMPRESSION_CODEC is invalid: {config.SPARK_SQL_PARQUET_COMPRESSION_CODEC} \ncheck https://spark.apache.org/docs/latest/configuration.html"
        )

    # check if theres a "k", "m", "g" or "t"
    if (not isinstance(config.SPARK_DRIVER_MEMORY, str)) or (
        config.SPARK_DRIVER_MEMORY[-1] not in ["k", "m", "g", "t"]
    ):
        raise ConfigurationValidationException(
            f"SPARK_DRIVER_MEMORY is invalid: {config.SPARK_DRIVER_MEMORY}"
        )

    # [0.5, 0.7]
    if (
        (not isinstance(config.SPARK_MEMORY_FRACTION, float))
        or (config.SPARK_MEMORY_FRACTION < 0.5)
        or (config.SPARK_MEMORY_FRACTION > 0.7)
    ):
        raise ConfigurationValidationException(
            f"SPARK_MEMORY_FRACTION is invalid: {config.SPARK_MEMORY_FRACTION} "
        )

    return config


# helper functions


def is_seq_of_str(value):
    return isinstance(value, (list, tuple)) and all(isinstance(x, str) for x in value)
