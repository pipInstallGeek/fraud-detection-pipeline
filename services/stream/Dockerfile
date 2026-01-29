# =============================================================================
# Stream Service Dockerfile - Production Optimized
# =============================================================================
# Multi-stage build for smaller image size
# Non-root user for security

# -----------------------------------------------------------------------------
# Stage 1: Build dependencies
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install dependencies
COPY pyproject.toml .
RUN pip install --user --no-cache-dir .

# -----------------------------------------------------------------------------
# Stage 2: Production image
# -----------------------------------------------------------------------------
FROM python:3.11-slim

# Labels for container metadata
LABEL maintainer="Marouane Bouziane"
LABEL description="Fraud Detection Pipeline - Stream Service"
LABEL version="0.1.0"

# Create non-root user
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /root/.local /home/appuser/.local

# Copy source code
COPY --chown=appuser:appuser stream_service/ ./stream_service/

# Copy config
COPY --chown=appuser:appuser config/ ./config/

# Set environment
ENV PATH="/home/appuser/.local/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app"

# Switch to non-root user
USER appuser

# Health check - verify service can be imported
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import stream_service" || exit 1

ENTRYPOINT ["python", "-m", "stream_service.main"]
