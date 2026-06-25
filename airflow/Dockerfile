FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

USER airflow

# Airflow-side deps only (Airbyte provider + helpers used by the DAG).
RUN pip install --no-cache-dir \
    "apache-airflow-providers-airbyte==3.6.0" \
    requests \
    pyyaml

# dbt lives in its OWN virtualenv, isolated from Airflow's dependency pins.
# Installing dbt into Airflow's env causes a shared-library tug-of-war
# (protobuf, typing_extensions, mashumaro): dbt wants modern versions but
# airflow 2.9.3 pins older ones, and they can't both win in one env. A
# separate venv gives each its own coherent dependency set.
# Only dbt-core needs an explicit pin: the adapters declare an open
# "dbt-core>=1.8.0a1", whose pre-release marker otherwise pulls dbt-core 2.0
# ("dbt Fusion"), which dropped the postgres adapter ("InvalidConfig dbt1005").
RUN python -m venv /home/airflow/dbt_venv && \
    /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core==1.8.2" \
        "dbt-postgres==1.8.2"
