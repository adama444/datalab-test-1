# Use a lightweight Python image
FROM python:3.11-slim

# Install system dependencies for PostGIS and Parquet processing
RUN apt-get update && apt-get install -y \
    libpq-dev gcc g++ \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy and install Python dependencies
# requirements.txt should include: pandas, pyarrow, sqlalchemy, psycopg2-binary
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingestion script
COPY ingest/validate_and_partition.py ./ingest/
COPY datalake/raw/ ./datalake/raw/

# Keep the container running or specify the entry point
CMD ["python", "./ingest/validate_and_partition.py"]