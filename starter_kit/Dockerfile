FROM nedbank-de-challenge/base:1.0

# Install any additional Python dependencies beyond the base image.
# Base already has: pyspark==3.5.0, delta-spark==3.1.0, pandas==2.1.0,
#                   pyarrow==14.0.0, pyyaml==6.0.1, duckdb==0.10.0
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline code and configuration.
# Data files are NOT copied — they are injected at runtime via Docker volumes.
COPY pipeline/ pipeline/
COPY config/ config/

# Entry point — complete pipeline, no interactive input required.
CMD ["python", "pipeline/run_all.py"]