# syntax=docker/dockerfile:1.6
#
# Multi-stage build for crypto_trading_system.
#
# Base image: python:3.11-slim (Debian Bookworm).
# Pin to a known-good digest in production by replacing the FROM tag with:
#     FROM python:3.11-slim@sha256:<digest>
# (Look up current digest with `docker pull python:3.11-slim` then `docker inspect ...`.)

# ────────────────────────────────────────────────────────────────────────────
# Stage 1 — builder: install build deps + python wheels into a venv
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Build toolchain only present in builder stage
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create an isolated venv we copy into the runtime image
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# ────────────────────────────────────────────────────────────────────────────
# Stage 2 — runtime: slim image, non-root user, healthcheck
# ────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TRADING_MODE=paper \
    PATH="/opt/venv/bin:$PATH"

# Runtime-only OS deps: curl needed for HEALTHCHECK probe
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Non-root user (UID/GID 10001) for least-privilege runtime
RUN useradd --create-home --uid 10001 --shell /bin/bash cts

WORKDIR /app

# Copy venv from builder stage
COPY --from=builder /opt/venv /opt/venv

# Copy application source (owned by cts)
COPY --chown=cts:cts . /app

# Pre-create writable data/log dirs as the runtime user
RUN mkdir -p /app/data/historical /app/data/cache /app/logs /app/runtime \
    && chown -R cts:cts /app/data /app/logs /app/runtime

USER cts

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8000/livez || exit 1

CMD ["python", "main.py"]
