import json
import logging
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
from services.posts_etl import PostsExtractor, PostsTransformer, PostsLoader
from services.minio_client import MinioHook


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

MINIO_BUCKET = Variable.get("minio_bucket", default_var="etl-data")

with DAG(
    'posts_etl',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    def extract_fn(ti):
        extractor = PostsExtractor()
        posts = extractor.daily_extract()
        if not posts:
                logging.warning("No posts extracted")
                return
        
        hook = MinioHook(minio_conn_id="minio_hf_conn")

        t = datetime.now(timezone.utc)
        ts = t.strftime("%Y%m%dT%H%M%S")

        raw_key = f"posts/raw/{ts}.json"

        hook.upload_json(posts, MINIO_BUCKET, raw_key)
        ti.xcom_push('posts_extract_key', raw_key)

    def transform_fn(ti):
        raw_key = ti.xcom_pull(task_ids='extract_task', key='posts_extract_key')

        if raw_key is None:
            logging.warning("No data")
            return

        hook = MinioHook(minio_conn_id="minio_hf_conn")

        raw_data = hook.download_json(MINIO_BUCKET, raw_key)

        transformer = PostsTransformer()
        transformed = [transformer.transform(p) for p in raw_data]

        t = datetime.now(timezone.utc)
        ts = t.strftime("%Y%m%dT%H%M%S")

        transform_key = f"posts/transform/{ts}.json"

        hook.upload_json(transformed, MINIO_BUCKET, transform_key)
        ti.xcom_push('posts_transform_key', transform_key)

    def load_fn(ti):
        transformed_key = ti.xcom_pull(task_ids='transform_task', key='posts_transform_key')

        if transformed_key is None:
            logging.warning("No data")
            return

        hook = MinioHook(minio_conn_id="minio_hf_conn")
        
        transformed = hook.download_json(MINIO_BUCKET, transformed_key)

        datetime_fields = ['published_at', "updated_at"]
        transformed = hook.deserialize_datetimes(transformed, datetime_fields)

        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        mongo_hook = MongoHook(mongo_conn_id='mongo_hf_conn')
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_hf_conn')

        loader = PostsLoader(pg_hook, mongo_hook, clickhouse_hook)
        loader.load_all(transformed, datetime.now(timezone.utc))

    extract_task = PythonOperator(task_id='extract_task', python_callable=extract_fn)
    transform_task = PythonOperator(task_id='transform_task', python_callable=transform_fn)
    load_task = PythonOperator(task_id='load_task', python_callable=load_fn)

    extract_task >> transform_task >> load_task
