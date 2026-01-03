FROM python:3.11-slim

# System deps (psycopg2-binary usually does not need libpq-dev)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY etl ./etl
COPY pipeline.py ./pipeline.py

# Make logs appear immediately (for cloud logs)
ENV PYTHONUNBUFFERED=1

# Run the ETL pipeline
CMD ["python", "pipeline.py"]

