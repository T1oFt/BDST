import json
from pathlib import Path
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone
from services.quaility import check_ods_dds_consistency
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 15),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    'quality_check',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    def run_quality_check_fn(ti):
        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        check_ods_dds_consistency(pg_hook)

    benchmark_task = PythonOperator(
        task_id='run_benchmark_task',
        python_callable=run_quality_check_fn,
    )