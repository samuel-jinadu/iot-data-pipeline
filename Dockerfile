FROM spark:4.2.0-scala2.13-java21-ubuntu AS builder

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*

COPY --from=docker.io/astral/uv:latest /uv /uvx /bin/

WORKDIR /pyspark-worker
COPY pyproject.toml uv.lock ./

# ---- Test stage ----
FROM builder AS test
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project
COPY . .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked 


# ---- Prod stage ----
FROM builder AS prod
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project
COPY iot_pipeline ./iot_pipeline
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev \
 && mkdir -p logs data/output \
 && chown -R spark:spark /pyspark-worker

ENV PYSPARK_PYTHON=/pyspark-worker/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/pyspark-worker/.venv/bin/python
USER spark
ENTRYPOINT ["uv", "run", "--frozen", "--no-sync", "iot_pipeline"]