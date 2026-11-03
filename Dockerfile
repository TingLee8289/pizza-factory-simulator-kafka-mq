FROM python:3.13-slim-bookworm

# uv from the official image (A7: if this tag does not resolve, pin the exact `uv --version`).
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

# Dependencies first (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Then the project itself
COPY src ./src
RUN uv sync --frozen --no-dev

RUN useradd --uid 10001 --create-home appuser
USER appuser

# One image for all services; docker-compose selects the service via `command`.
CMD ["python", "-m", "pizza.common.bootstrap"]
