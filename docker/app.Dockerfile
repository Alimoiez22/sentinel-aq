# The "app" image: our Python package, dbt and the dev tools, used by `make test`,
# `make fixtures` and (later) every pipeline command run by hand.
# Base images are pinned by digest: a tag can move, a digest cannot (resolved 2026-11-06).

# uv 0.12.23
FROM ghcr.io/astral-sh/uv@sha256:61d393e44e249f2e4b526b6c7ddcecce245946826e608e11c93ad4f5bba55b21 AS uv

# python:3.12-slim
FROM python@sha256:4840d514c583a4d3d8cad3940e68dee6f0f6f3635c12fcd0a3d74c21a5b93a54

COPY --from=uv /uv /uvx /bin/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Tool caches go to /tmp, so nothing is written into the bind-mounted repo.
    RUFF_CACHE_DIR=/tmp/ruff_cache \
    MYPY_CACHE_DIR=/tmp/mypy_cache

WORKDIR /workspace

# Dependencies first (cached layer), then the project itself. The cache mount keeps
# downloaded wheels between builds, so a failed or repeated build does not download
# about 250 MB again (the connection measured ~200 KB/s on 2026-11-06).
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,id=sentinel-uv,target=/root/.cache/uv \
    uv sync --frozen --no-install-project
COPY src ./src
RUN --mount=type=cache,id=sentinel-uv,target=/root/.cache/uv \
    uv sync --frozen

# Fail the build if the package is broken (see the note in airflow.Dockerfile).
RUN sentinel --version && python -c "import sentinel.cli, sentinel.settings, sentinel.log"

RUN useradd --create-home --uid 1000 app
USER app
