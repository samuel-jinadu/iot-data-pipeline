#!/bin/bash
set -e

echo "Running pyspark..."
uv run iot_pipeline yaml/dev.yaml