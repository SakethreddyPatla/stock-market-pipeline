FROM apache/airflow:2.10.4-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    dbt-snowflake \
    requests \
    python-dotenv
