import json
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
from services.ods.models_etl import ModelsExtractor, ModelsTransformer, ModelsLoader
from services.minio_client import MinioHook
import logging


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

MINIO_BUCKET = Variable.get("minio_bucket", default_var="etl-data")

with DAG(
    'models_etl',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    def extract_fn(ti):
        try:
            extractor = ModelsExtractor(limit=50)
            models = extractor.extract()
            if not models:
                logging.warning("No models extracted")
                return
            
            hook = MinioHook(minio_conn_id="minio_hf_conn")

            t = datetime.now(timezone.utc)
            ts = t.strftime("%Y%m%dT%H%M%S")

            raw_key = f"models/raw/{ts}.json"
                
            hook.upload_json([m.__dict__ for m in models], MINIO_BUCKET, raw_key)
            ti.xcom_push('models_extract_key', raw_key)
            logging.info(f"Successfully extracted {len(models)} models")
        except Exception as e:
            logging.error(f"Extraction failed: {str(e)}")
            raise

    def transform_fn(ti):
        raw_key = ti.xcom_pull(task_ids='extract_task', key='models_extract_key')
        if raw_key is None:
            logging.warning("No data")
            return
        
        hook = MinioHook(minio_conn_id="minio_hf_conn")

        raw_data = hook.download_json(MINIO_BUCKET, raw_key)

        transformer = ModelsTransformer()
        transformed = [transformer.transform(m) for m in raw_data]

        t = datetime.now(timezone.utc)
        ts = t.strftime("%Y%m%dT%H%M%S")

        transform_key = f"models/transform/{ts}.json"

        hook.upload_json(transformed, MINIO_BUCKET, transform_key)
        ti.xcom_push('models_transform_key', transform_key)

    def load_fn(ti):
        transformed_key = ti.xcom_pull(task_ids='transform_task', key='models_transform_key')

        if transformed_key is None:
            logging.warning("No data")
            return
        
        hook = MinioHook(minio_conn_id="minio_hf_conn")

        transformed = hook.download_json(MINIO_BUCKET, transformed_key)

        datetime_fields = ['created_at']
        transformed = hook.deserialize_datetimes(transformed, datetime_fields)

        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        mongo_hook = MongoHook(mongo_conn_id='mongo_hf_conn')
        clickhouse_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_hf_conn')

        loaded_at = datetime.now(timezone.utc)

        loader = ModelsLoader(pg_hook, mongo_hook, clickhouse_hook)
        loader.load_all(transformed, loaded_at)

        ti.xcom_push('models_etl_loaded_at', loaded_at.isoformat())

    extract_task = PythonOperator(task_id='extract_task', python_callable=extract_fn)
    transform_task = PythonOperator(task_id='transform_task', python_callable=transform_fn)
    load_task = PythonOperator(task_id='load_task', python_callable=load_fn)

    trigger_dds = TriggerDagRunOperator(
        task_id='trigger_models_dds',
        trigger_dag_id='models_dds',
        conf={"loaded_at": "{{ ti.xcom_pull(task_ids='load_task', key='models_etl_loaded_at') }}"},
    )

    extract_task >> transform_task >> load_task >> trigger_dds
