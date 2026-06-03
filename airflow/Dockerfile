FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir --use-deprecated=legacy-resolver \
    "dbt-postgres>=1.8.0,<2.0.0" \
    "dbt-duckdb>=1.8.0,<2.0.0" \
    "apache-airflow-providers-airbyte==3.6.0" \
    requests \
    pyyaml
