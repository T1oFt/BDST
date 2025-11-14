import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.clickhouse.hooks.clickhouse import ClickHouseHook
from datetime import datetime, timedelta
from dags.services.papers_etl import PapersExtractor, PapersTransformer, PapersLoader
from dags.services.minio_client import MinioClient

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

minio_client = MinioClient()
MINIO_RAW_KEY = 'papers/raw_papers.json'
MINIO_TRANSFORMED_KEY = 'papers/transformed_papers.json'

with DAG(
    'papers_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def extract_fn(ti):
        extractor = PapersExtractor()
        papers = extractor.extract()
        minio_client.upload_json([p.__dict__ for p in papers], MINIO_RAW_KEY)
        ti.xcom_push('minio_key', MINIO_RAW_KEY)

    def transform_fn(ti):
        raw_key = ti.xcom_pull(task_ids='extract_task', key='minio_key')
        raw_data = minio_client.download_json(raw_key)
        transformer = PapersTransformer()
        transformed = [transformer.transform(p) for p in raw_data]
        minio_client.upload_json(transformed, MINIO_TRANSFORMED_KEY)
        ti.xcom_push('minio_key', MINIO_TRANSFORMED_KEY)

    def load_fn(ti):
        transformed_key = ti.xcom_pull(task_ids='transform_task', key='minio_key')
        transformed = minio_client.download_json(transformed_key)

        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        mongo_hook = MongoHook(conn_id='mongo_hf_conn')
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_hf_conn')

        loader = PapersLoader(pg_hook, mongo_hook, clickhouse_hook)
        loader.load_postgresql(transformed)
        loader.load_mongodb(transformed)
        loader.load_clickhouse(transformed)

    extract_task = PythonOperator(task_id='extract_task', python_callable=extract_fn)
    transform_task = PythonOperator(task_id='transform_task', python_callable=transform_fn)
    load_task = PythonOperator(task_id='load_task', python_callable=load_fn)

    extract_task >> transform_task >> load_task
