FROM python:3.11-slim

# Set working directory
WORKDIR /predicateAutomate/app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY predicateAutomate/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY predicateAutomate/ .

# Create output directories
RUN mkdir -p /predicateAutomate/usa_drug/output /predicateAutomate/singapore_drug/output

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

# Default command - use app.py orchestrator which respects config.json
# This enables the container to be used in cron jobs and respects module enable/disable settings
CMD ["python", "-u", "app.py", "all"]

