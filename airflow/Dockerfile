FROM apache/airflow:2.9.3-python3.11

# Install system tools (optional but handy)
USER root
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*
USER airflow

# Python deps for your pipeline + dbt-bigquery
# (pin dbt to your local version family)
RUN pip install --no-cache-dir \
    google-cloud-storage google-cloud-bigquery google-api-python-client python-dotenv \
    dbt-bigquery==1.10.*

# Keep airflow UID happy inside bind mounts
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False