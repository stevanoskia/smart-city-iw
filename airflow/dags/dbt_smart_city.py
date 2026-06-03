from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_smart_city',
    default_args=default_args,
    description='Run dbt staging models every hour',
    schedule_interval='@hourly',
    start_date=datetime(2026, 6, 3),
    catchup=False,
    tags=['dbt', 'smart-city'],
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'source ~/airflow-venv/bin/activate && '
            'dbt run '
            '--project-dir /mnt/d/IWConnect/smart-city-iw/dbt/smart_city '
            '--profiles-dir ~/.dbt'
        ),
    )
