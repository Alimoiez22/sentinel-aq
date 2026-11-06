# Airflow image with our package in its OWN virtual environment (ADR-003).
# Airflow pins hundreds of libraries through its constraints file. Installing dbt and
# polars into Airflow's environment would fight those pins, so tasks call
# /opt/sentinel-venv/bin/sentinel instead, built from the same uv.lock as the app image.

# uv 0.12.23
FROM ghcr.io/astral-sh/uv@sha256:61d393e44e249f2e4b526b6c7ddcecce245946826e608e11c93ad4f5bba55b21 AS uv

# apache/airflow:3.3.2-python3.12
FROM apache/airflow@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1

COPY --from=uv /uv /bin/uv

# The venv is installed as root and is read-only for the `airflow` user at run time:
# tasks can execute our code but cannot modify it.
USER root

ENV UV_PROJECT_ENVIRONMENT=/opt/sentinel-venv \
    UV_CACHE_DIR=/root/.cache/uv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    # The image's own interpreter (checked 2026-11-06: /usr/python/bin/python3.12, 3.12.14).
    # uv warns that Airflow's VIRTUAL_ENV is ignored; that is intended.
    UV_PYTHON=/usr/python/bin/python3.12

WORKDIR /opt/sentinel-src
COPY pyproject.toml uv.lock README.md ./
# Same cache id and path as the app image, so wheels download only once.
# (The base image sets UV_CACHE_DIR=/tmp/.cache/uv; overridden above to match.)
RUN --mount=type=cache,id=sentinel-uv,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,id=sentinel-uv,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# Fail the build if the package is broken. On 2026-11-06 a build during an unstable
# Docker engine copied every source file as 0 bytes and still "succeeded".
RUN /opt/sentinel-venv/bin/sentinel --version \
    && /opt/sentinel-venv/bin/python -c "import sentinel.cli, sentinel.settings, sentinel.log"

# Restore the base image's runtime settings.
ENV UV_CACHE_DIR=/tmp/.cache/uv
USER airflow
WORKDIR /opt/airflow
