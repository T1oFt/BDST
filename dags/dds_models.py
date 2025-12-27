import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from services.dds.models import (
    ModelsDataExtractor,
    ModelsDataTransformer,
    ModelsDataLoader,
)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='models_dds',
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    def dds_load_fn(dag_run, **_):
        loaded_at_iso = dag_run.conf.get("loaded_at")
        if not loaded_at_iso:
            raise ValueError("models_etl_loaded_at is not set in XCom")

        loaded_at = datetime.fromisoformat(loaded_at_iso)

        pg_hook = PostgresHook(postgres_conn_id='pg_hf_conn')
        extractor = ModelsDataExtractor(pg_hook=pg_hook)
        df = extractor.get_models_by_loaded_at(
            loaded_at.strftime('%Y-%m-%d %H:%M:%S.%f')
        )

        if df.empty:
            logging.warning(f"No models found for {loaded_at.isoformat()}")
            return
        
        logging.info(f"Found {len(df)} models for {loaded_at.isoformat()}")


        transformer = ModelsDataTransformer()
        (
            dim_models_raw,
            fact_models_raw,
            unique_owners,
            unique_dates,
            unique_times,
            unique_datasets,
            model_languages,
            model_libraries,
            model_tasks,
            model_datasets,
        ) = transformer.transform_models_df(df)

        logging.info(f"Transformed {len(dim_models_raw)} models")

        loader = ModelsDataLoader(postgres_conn_id='pg_hf_conn')
        loader.load_all(
            dim_models_raw=dim_models_raw,
            fact_models_raw=fact_models_raw,
            unique_owners=unique_owners,
            unique_dates=unique_dates,
            unique_times=unique_times,
            unique_datasets=unique_datasets,
            model_languages=model_languages,
            model_libraries=model_libraries,
            model_tasks=model_tasks,
            model_datasets=model_datasets,
            loaded_at=loaded_at,
        )

        logging.info(f"Loaded {len(dim_models_raw)} models")

    dds_load_task = PythonOperator(
        task_id='dds_load_task',
        python_callable=dds_load_fn,
    )

    trigger_ads = TriggerDagRunOperator(
        task_id='trigger_models_ads',
        trigger_dag_id='models_ads',
    )

    dds_load_task >> trigger_ads
